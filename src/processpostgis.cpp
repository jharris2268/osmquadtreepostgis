/*****************************************************************************
 *
 * This file is part of osmquadtreepostgis
 *
 * Copyright (C) 2019 James Harris
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 *****************************************************************************/

#include "processpostgis.hpp"
#include "oqt/geometry/elements/waywithnodes.hpp"

#include "oqt/elements/header.hpp"
#include "oqt/pbfformat/readfileblocks.hpp"
#include "oqt/pbfformat/writepbffile.hpp"
#include "oqt/pbfformat/writeblock.hpp"

#include "oqt/sorting/sortblocks.hpp"

#include "oqt/utils/logger.hpp"

#include "oqt/utils/threadedcallback.hpp"
#include "oqt/utils/multithreadedcallback.hpp"
#include "oqt/utils/splitcallback.hpp"

namespace oqt {
namespace geometry {




block_callback make_pack_csvblocks_callback(block_callback cb, std::function<void(std::shared_ptr<CsvBlock>)> wr, PackCsvBlocks::tagspec tags,bool with_header,bool as_binary, table_alloc_func alloc_func, bool split_multipolygons, bool validate_geometry) {
    auto pc = make_pack_csvblocks(tags,with_header,as_binary, alloc_func, split_multipolygons,validate_geometry);
    return [cb, wr, pc](PrimitiveBlockPtr bl) {
        if (!bl) {
            //std::cout << "pack_csvblocks done" << std::endl;
            wr(std::shared_ptr<CsvBlock>());
            if (cb) {
                cb(bl);
            }
            return;
        }
        if (cb) { cb(bl); }
        //std::cout << "call pack_csvblocks ... " << std::endl;
        auto cc = pc->call(bl);
        //std::cout << "points.size()=" << cc->points.size() << ", lines.size()=" << cc->lines.size() << ", polys.size()=" << cc->polys.size() << std::endl;
        wr(cc);
        return;
    };
}
                
                            

std::vector<block_callback> write_to_postgis_callback(
    std::vector<block_callback> callbacks, size_t numchan,
    const std::string& connection_string, const std::string& table_prfx,
    const PackCsvBlocks::tagspec& coltags,
    bool with_header, bool as_binary,
    table_alloc_func alloc_func,
    bool split_multipolygons,
    bool validate_geometry) {
        
    
    auto writers = multi_threaded_callback<CsvBlock>::make(make_postgiswriter_callback(connection_string, table_prfx, with_header,as_binary), numchan);
    //auto writers = threaded_callback<CsvBlock>::make(make_postgiswriter_callback(connection_string, table_prfx, with_header,false), numchan);
    
    std::vector<block_callback> res(numchan);
    
    for (size_t i=0; i < numchan; i++) {
        
        
        auto writer_i = writers[i];
        block_callback cb;
        if (!callbacks.empty()) {
            cb = callbacks[i];
        }
        res[i]=make_pack_csvblocks_callback(cb, writer_i, coltags, with_header,as_binary,alloc_func,split_multipolygons,validate_geometry);
    }
    
    return res;
}
        
block_callback write_to_postgis_callback_nothread(
    block_callback callback,
    const std::string& connection_string, const std::string& table_prfx,
    const PackCsvBlocks::tagspec& coltags,
    bool with_header, bool as_binary,
    table_alloc_func alloc_func,
    bool split_multipolygons,
    bool validate_geometry) {
        
    
    auto writer = make_postgiswriter_callback(connection_string, table_prfx,with_header,as_binary);
    return make_pack_csvblocks_callback(callback,writer,coltags,with_header,as_binary,alloc_func,split_multipolygons,validate_geometry);
}




mperrorvec process_geometry_postgis(const GeometryParameters& params, const PostgisParameters& postgis, block_callback wrapped) {
    
    if (postgis.connstring.empty()) {
        throw std::domain_error("must specify postgis connection string");
    }
    
    mperrorvec errors_res;
    
    
    if (!wrapped) {
        wrapped = make_geomprogress(params.locs);
    }
    std::vector<block_callback> writer = multi_threaded_callback<PrimitiveBlock>::make(wrapped,params.numchan);
    
        
    if (params.outfn!="") {
        writer = pack_and_write_callback(writer, params.outfn, params.indexed, params.box, params.numchan, true, true, true);
        
    }
    
    bool header = (!postgis.use_binary) ? true : false;
    writer = write_to_postgis_callback(writer, params.numchan, postgis.connstring, postgis.tableprfx, postgis.coltags, header, postgis.use_binary,postgis.alloc_func,postgis.split_multipolygons,postgis.validate_geometry);
    
    auto addwns = process_geometry_blocks(
            writer, params,
            [&errors_res](mperrorvec& ee) { errors_res.errors.swap(ee.errors); }
    );
    
    read_blocks_merge(params.filenames, addwns, params.locs, params.numchan, nullptr, ReadBlockFlags::Empty, 1<<14);
      
    
    return errors_res;

}



mperrorvec process_geometry_postgis_nothread(const GeometryParameters& params, const PostgisParameters& postgis, block_callback callback) {

    if (postgis.connstring.empty()) {
        throw std::domain_error("must specify postgis connection string");
    }
    
    mperrorvec errors_res;
    
    
    
    block_callback writer=callback;
    if (params.outfn!="") {
        writer = pack_and_write_callback_nothread(callback, params.outfn, params.indexed, params.box, true, true, true);
    }
   
    
    bool header = (!postgis.use_binary) ? true : false;
    writer = write_to_postgis_callback_nothread(writer, postgis.connstring, postgis.tableprfx, postgis.coltags, header, postgis.use_binary,postgis.alloc_func,postgis.split_multipolygons,postgis.validate_geometry);
    
    block_callback addwns = process_geometry_blocks_nothread(
            writer, params,
            [&errors_res](mperrorvec& ee) { errors_res.errors.swap(ee.errors); }
    );
    
    read_blocks_merge_nothread(params.filenames, addwns, params.locs, nullptr, ReadBlockFlags::Empty);
      
    
    return errors_res;

}











mperrorvec process_geometry_csvcallback(const GeometryParameters& params,
    const PostgisParameters& postgis,
    block_callback callback,
    std::function<void(std::shared_ptr<CsvBlock>)> csvblock_callback) {
        
    
    mperrorvec errors_res;
    
    auto cb=make_pack_csvblocks_callback(callback,csvblock_callback,postgis.coltags, true, postgis.use_binary,postgis.alloc_func,postgis.split_multipolygons,postgis.validate_geometry);
    auto csvcallback = multi_threaded_callback<PrimitiveBlock>::make(cb,params.numchan);
       
    
    auto addwns = process_geometry_blocks(
            csvcallback, params,
            [&errors_res](mperrorvec& ee) { errors_res.errors.swap(ee.errors); }
    );
    
    read_blocks_merge(params.filenames, addwns, params.locs, params.numchan, nullptr, ReadBlockFlags::Empty, 1<<14);
    
    return errors_res;

}

mperrorvec process_geometry_csvcallback_nothread(const GeometryParameters& params,
    const PostgisParameters& postgis,
    block_callback callback,
    std::function<void(std::shared_ptr<CsvBlock>)> csvblock_callback) {
        
    
    mperrorvec errors_res;
    
    block_callback csvcallback = make_pack_csvblocks_callback(callback,csvblock_callback,postgis.coltags, true, postgis.use_binary,postgis.alloc_func,postgis.split_multipolygons,postgis.validate_geometry);
    
    block_callback addwns = process_geometry_blocks_nothread(
            csvcallback, params,
            [&errors_res](mperrorvec& ee) { errors_res.errors.swap(ee.errors); }
    );
    
    read_blocks_merge_nothread(params.filenames, addwns, params.locs, nullptr, ReadBlockFlags::Empty);
      
    
    return errors_res;

}

}
}
