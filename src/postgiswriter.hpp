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

#ifndef GEOMETRY_POSTGISWRITER_HPP
#define GEOMETRY_POSTGISWRITER_HPP

#include "oqt/elements/block.hpp"
#include <map>


namespace oqt {
namespace geometry {



class CsvRows {
    
    public:
        CsvRows(bool is_binary_);
        
        bool is_binary() { return _is_binary; }
        
        void finish();
        void add(const std::string row);
        std::string at(int i) const;
        int size() const;
        
        const std::string& data_blob() const { return data; }
        
    private:
        bool _is_binary;
        
        std::string data;
        std::vector<size_t> poses;
};

class CsvBlock {
    
    
    public:
        CsvBlock(bool is_binary_) : is_binary(is_binary_) {}
        virtual ~CsvBlock() {}
        
        CsvRows& get(const std::string& tab) {
            if (rows_.count(tab)==0) {
                rows_.insert(std::make_pair(tab,CsvRows(is_binary)));
            }
            return rows_.at(tab);
        }
        
        void finish() {
            for (auto& r: rows_) {
                r.second.finish();
            }
        }
        
        const std::map<std::string,CsvRows>& rows() const { return rows_; } 
    
    private:
        bool is_binary;
        std::map<std::string, CsvRows> rows_;
};

enum class ColumnType {
    Text,
    BigInteger,
    Integer,
    Double,
    Hstore,
    Json,
    TextArray,
    PointGeometry,
    LineGeometry,
    PolygonGeometry
};

enum class ColumnSource {
    OsmId,
    Part,
    ObjectQuadtree,
    BlockQuadtree,
    Tag,
    OtherTags,
    Layer,
    ZOrder,
    MinZoom,
    Length,
    Area,
    Geometry
};


struct ColumnSpec {
    ColumnSpec(const std::string& name_, ColumnType type_, ColumnSource source_) : name(name_), type(type_), source(source_) {}
    std::string name;
    ColumnType type;
    ColumnSource source;
};

struct TableSpec {
    TableSpec(const std::string& table_name_) : table_name(table_name_) {}
    std::string table_name;
    std::vector<ColumnSpec> columns;
};

   
class PackCsvBlocks {
    public:
        //typedef std::vector<std::tuple<std::string,bool,bool,bool>> tagspec;
        
        
        typedef std::vector<TableSpec> tagspec;
        
        
        virtual std::shared_ptr<CsvBlock> call(PrimitiveBlockPtr bl) = 0;
        virtual ~PackCsvBlocks() {}
};


typedef std::function<std::vector<std::string>(ElementPtr)> table_alloc_func;

std::vector<std::string> default_table_alloc(ElementPtr geom);


std::shared_ptr<PackCsvBlocks> make_pack_csvblocks(const PackCsvBlocks::tagspec& tags, bool with_header, bool binary_format, table_alloc_func table_alloc);

class PostgisWriter {
    public:
        
        virtual void finish()=0;
        virtual void call(std::shared_ptr<CsvBlock> bl)=0;
        virtual ~PostgisWriter() {}
};

std::shared_ptr<PostgisWriter> make_postgiswriter(
    const std::string& connection_string,
    const std::string& table_prfx,
    bool with_header, bool binary_format);

std::function<void(std::shared_ptr<CsvBlock>)> make_postgiswriter_callback(
    const std::string& connection_string,
    const std::string& table_prfx,
    bool with_header, bool binary_format);

}}  


#endif 
