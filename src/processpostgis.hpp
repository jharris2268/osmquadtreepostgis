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

#ifndef GEOMETRY_PROCESSPOSTGIS_HPP
#define GEOMETRY_PROCESSPOSTGIS_HPP

#include "oqt/geometry/process.hpp"
#include "postgiswriter.hpp"

namespace oqt {
namespace geometry {


struct PostgisParameters {
    
    PostgisParameters()
        : connstring(""), tableprfx(""), use_binary(false), alloc_func(default_table_alloc), split_multipolygons(false), validate_geometry(false), round_geometry(false) {}
        
    
    std::string connstring;
    std::string tableprfx;
    PackCsvBlocks::tagspec coltags;
    bool use_binary;
    table_alloc_func alloc_func;
    
    bool split_multipolygons;
    bool validate_geometry;
    bool round_geometry;
};


mperrorvec process_geometry_postgis(const GeometryParameters& params, const PostgisParameters& postgis, block_callback cb);
mperrorvec process_geometry_postgis_nothread(const GeometryParameters& params, const PostgisParameters& postgis, block_callback cb);

mperrorvec process_geometry_csvcallback(const GeometryParameters& params,
    const PostgisParameters& postgis, 
    block_callback callback,
    std::function<void(std::shared_ptr<CsvBlock>)> csvblock_callback);


mperrorvec process_geometry_csvcallback_nothread(const GeometryParameters& params,
    const PostgisParameters& postgis, 
    block_callback callback,
    std::function<void(std::shared_ptr<CsvBlock>)> csvblock_callback);


}
}
#endif
