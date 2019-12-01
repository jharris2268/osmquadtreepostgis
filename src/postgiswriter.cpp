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

#include "postgiswriter.hpp"

#include "oqt/geometry/utils.hpp"
#include "oqt/geometry/elements/ring.hpp"
#include "oqt/geometry/elements/point.hpp"
#include "oqt/geometry/elements/linestring.hpp"
#include "oqt/geometry/elements/simplepolygon.hpp"
#include "oqt/geometry/elements/complicatedpolygon.hpp"
#include "oqt/geometry/elements/waywithnodes.hpp"
#include "oqt/utils/logger.hpp"

#include "oqt/utils/pbf/protobuf.hpp"
#include "oqt/utils/pbf/fixedint.hpp"

#include <sstream>
#include <fstream>

#include <iostream>
#include <map>
#include <postgresql/libpq-fe.h>
#include "picojson.h"
#include "validategeoms.hpp"

namespace oqt {
namespace geometry {

const char quote = '\x01';
const char delim = '\x02';


struct QuoteString {
    
    const std::string& val;
};

std::ostream& operator<<(std::ostream& strm, const QuoteString& q) {
    strm << quote;
    for (auto c : q.val) {
        if (c=='\n') {
            strm << "\\n";
        } else {
            /*if (c=='"') {
                strm << '"';
            }*/
            strm << c;
        }
    }
    strm << quote;
    return strm;
}

void quotestring(std::ostream& strm, const std::string& val) {
    strm << QuoteString{val};
}

std::string text_string(const std::string& val) {
    std::stringstream ss;
    ss << QuoteString{val};
    return ss.str();
}

struct DoublePrec {
    double v;
    int dp;
};

std::ostream& operator<<(std::ostream& strm, const DoublePrec& d) {
    strm << std::fixed << std::setprecision(d.dp) << d.v;
    return strm;
}

std::string double_string(double v) {
    std::stringstream ss;
    ss << DoublePrec{v,1};
    return ss.str();
}

std::ostream& writestring(std::ostream& strm, const std::string& val) {
    if (!val.empty()) {
        quotestring(strm,val);
    }
    
    return strm;
}


void json_quotestring(std::ostream& strm, const std::string& val) {
    strm << '"';
    for (auto c : val) {
        if (c=='\n') {
            strm << "\\\n";
        } else if (c=='"') {
            strm << '\\' << '"';
        } else if (c=='\t') {
            strm << "\\\t";
        } else if (c=='\r') {
            strm << "\\\r";
        } else if (c=='\\') {
            strm << "\\\\";
        } else {
            strm << c;
        }
    }
    strm << '"';
}



std::string pack_jsontags(const tagvector& tags) {
    std::stringstream strm;
    strm << "{";
    bool isf=true;
    for (const auto& t: tags) {
        if (!isf) { strm << ", "; }
        json_quotestring(strm,t.key);
        strm << ": ";
        json_quotestring(strm,t.val);
        isf=false;
    }
    strm << "}";
    return strm.str();
}

size_t _write_data(std::string& data, size_t pos, const std::string& v) {
    std::copy(v.begin(),v.end(),data.begin()+pos);
    pos+=v.size();
    return pos;
}
    

std::string prep_tags(std::stringstream& strm, const std::map<std::string,size_t>& tags, ElementPtr obj, bool other_tags, bool asjson) {
    std::vector<std::string> tt(tags.size(),"");
    
    tagvector others;
    
    if (!obj->Tags().empty()) {
        for (const auto& tg:obj->Tags()) {
            auto it=tags.find(tg.key);
            if (it!=tags.end()) {
                if (it->second >= tags.size()) {
                    Logger::Message() << "tag out of bounds?? " << tg.key << " " << tg.val << "=>" << it->second << "/" << tt.size();
                    throw std::domain_error("tag out of bounds");
                }
                tt.at(it->second)=tg.val;
            } else if (other_tags) {
                others.push_back(tg);
            }
        }
    }
    
    
    for (const auto& t:tt) {
        writestring(strm,t) << delim;
        
    }
    if (other_tags) {
        if (asjson) {
            return pack_jsontags_picojson(others);
        } else {
            return pack_hstoretags(others);
        }
    }
    return std::string();            
}

std::pair<std::string,std::string> prep_tags_binary(const std::map<std::string,size_t>& tags, ElementPtr obj, bool other_tags) {
    std::vector<std::string> tt(tags.size(),"");
    
    tagvector others;
    size_t len=tags.size()*4;
    
    if (!obj->Tags().empty()) {
        for (const auto& tg:obj->Tags()) {
            auto it=tags.find(tg.key);
            if (it!=tags.end()) {
                if (it->second >= tags.size()) {
                    Logger::Message() << "tag out of bounds?? " << tg.key << " " << tg.val << "=>" << it->second << "/" << tt.size();
                    throw std::domain_error("tag out of bounds");
                }
                tt.at(it->second)=tg.val;
                len+=tg.val.size();
            } else if (other_tags) {
                others.push_back(tg);
            }
        }
    }
    
    std::string out(len,0);
    size_t pos=0;
    for (const auto& t:tt) {
        if (t.empty()) {
            pos=write_int32(out,pos,-1);
        } else {
            pos=write_int32(out,pos,t.size());
            pos=_write_data(out,pos,t);
        }
    }
    
    std::string hstore;
    if (other_tags) {
        hstore=pack_hstoretags_binary(others);
    }
    return std::make_pair(out,hstore);
}
        


std::string as_hex(const std::string& str) {
    std::stringstream ss;
    for (unsigned char c : str) {
        ss << std::hex << std::setfill('0') << std::setw(2) << (size_t) c;
    }

    return ss.str();

}

CsvRows::CsvRows(bool is_binary_) : _is_binary(is_binary_) {
    data.reserve(1024*1024);
    poses.reserve(1000);
    if (_is_binary) {
        data += std::string("PGCOPY\n\xff\r\n\x00\x00\x00\x00\x00\x00\x00\x00\x00",19);
    }
}
    
void CsvRows::add(const std::string row) {
    if ((row.size() + data.size()) > data.capacity()) {
        data.reserve(data.capacity()+1024*1024);
    }    
    if (poses.size()==poses.capacity()) {
        poses.reserve(poses.capacity()+1000);
    }
    
    if (poses.empty()) { poses.push_back(data.size()); }
    
    //pos = _write_data(data, pos, row);
    data += row;
    poses.push_back(data.size());
}

std::string CsvRows::at(int i) const {
    if (i<0) { i += size(); }
    if (i >= size()) { throw std::range_error("out of range"); }
    size_t p = poses[i];
    size_t q = poses[i+1];
    return data.substr(p,q-p);
}

void CsvRows::finish() {
    if (_is_binary) {
        data += "\xff\xff";        
    }
    
}

int CsvRows::size() const {
    if (poses.empty()) { return 0; }
    return poses.size()-1;
}

std::vector<std::string> default_table_alloc(ElementPtr ele) {
    
    if (ele->Type() == ElementType::Point) { return {"point"}; }
    if (ele->Type() == ElementType::Linestring) { return {"line"}; }
    if (ele->Type() == ElementType::SimplePolygon) { return {"polygon"}; }
    if (ele->Type() == ElementType::ComplicatedPolygon) { return {"polygon"}; }
    return {};
}
    

class PackCsvBlocksTableBase {
    public:
        virtual std::string header()=0;
        virtual std::string call(ElementPtr ele, int64 block_qt)=0;
        virtual std::string call_complicatedpolygon_part(std::shared_ptr<geometry::ComplicatedPolygon> ele, size_t part, int64 block_qt)=0;
};


std::string pack_csv_row(const std::vector<std::string>& current) {
    std::stringstream ss;
    
    for (size_t i=0; i < current.size(); i++) {
        if (i>0) {
            ss << delim;
        }
        ss << current[i];
    }
    ss << '\n';
    return ss.str();
}   
    
    
    


class PackCsvBlocksTable : public PackCsvBlocksTableBase {
    public:
        PackCsvBlocksTable(const TableSpec& table_spec_)
         : table_spec(table_spec_), othertags_col(-1) {
            
            for (size_t i=0; i<table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                                
                if (col.source == ColumnSource::OtherTags) {
                    othertags_col=i;
                }
                if (col.source == ColumnSource::Tag) {
                    tag_cols[col.name] = i;
                }
                
            }
            
            
             
        }
        
        virtual ~PackCsvBlocksTable() {}
        
        std::string header() {
            
            std::vector<std::string> current(table_spec.columns.size());
            
            for (size_t i=0; i<table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                current[i] = col.name;
            }
            return pack_csv_row(current);
            
        }
        
        std::string call(ElementPtr ele, int64 block_qt) {
            if (!ele) { throw std::domain_error("??"); }
            
            std::vector<std::string> res(table_spec.columns.size());
            
            if (ele->Type() == ElementType::Point) {
                auto pt = std::dynamic_pointer_cast<geometry::Point>(ele);
                populate_point(pt, block_qt, res);
            } else if (ele->Type() == ElementType::Linestring) {
                auto ln = std::dynamic_pointer_cast<geometry::Linestring>(ele);
                populate_line(ln, block_qt, res);
            } else if (ele->Type() == ElementType::SimplePolygon) {
                auto py = std::dynamic_pointer_cast<geometry::SimplePolygon>(ele);
                populate_simplepolygon(py, block_qt, res);
            } else if (ele->Type() == ElementType::ComplicatedPolygon) {
                auto py = std::dynamic_pointer_cast<geometry::ComplicatedPolygon>(ele);
                populate_complicatedpolygon(py, block_qt, res);
            }
            return pack_csv_row(res);
            
        }
        
        std::string call_complicatedpolygon_part(std::shared_ptr<geometry::ComplicatedPolygon> ele, size_t part, int64 block_qt) {
            if (!ele) { throw std::domain_error("??"); }
            
            std::vector<std::string> res(table_spec.columns.size());
            populate_complicatedpolygon_part(ele, part, block_qt, res);
            return pack_csv_row(res);
        }
    
    private:
        TableSpec table_spec;
        int othertags_col;
        std::map<std::string,size_t> tag_cols;
        
        
        
        
        std::string add_tags(const tagvector& tags, std::vector<std::string>& current) {
            tagvector oo;
            for (const auto& tg: tags) {
                if (tag_cols.count(tg.key)) {
                    current[tag_cols.at(tg.key)] = text_string(tg.val);
                } else if (othertags_col>=0) {
                    oo.push_back(tg);
                }
            }
            
            if (othertags_col>=0) {
                return pack_hstoretags(oo);
            }
            return "";
        }
            
            
        
        void populate_point(std::shared_ptr<geometry::Point> ele, int64 block_qt, std::vector<std::string>& current) {
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::to_string(ele->Id());
                } else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::to_string(ele->Quadtree());
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::to_string(block_qt);
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>0) {
                        current[i] = std::to_string(ele->MinZoom());
                    }
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = as_hex(ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = add_tags(ele->Tags(), current);
            } else {
                add_tags(ele->Tags(), current);
            }
            
            
        }
        
        void populate_line(std::shared_ptr<geometry::Linestring> ele, int64 block_qt, std::vector<std::string>& current) {
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::to_string(ele->Id());
                } else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::to_string(ele->Quadtree());
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::to_string(block_qt);
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::to_string(ele->MinZoom());
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Length) {
                    current[i]=double_string(ele->Length());
                    
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = as_hex(ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = add_tags(ele->Tags(), current);
            } else {
                add_tags(ele->Tags(), current);
            }
            
        
        }
                
        void populate_simplepolygon(std::shared_ptr<geometry::SimplePolygon> ele, int64 block_qt, std::vector<std::string>& current) {
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::to_string(ele->Id());
                } else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::to_string(ele->Quadtree());
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::to_string(block_qt);
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::to_string(ele->MinZoom());
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Area) {
                    current[i]=double_string(ele->Area());
                    
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = as_hex(ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = add_tags(ele->Tags(), current);
            } else {
                add_tags(ele->Tags(), current);
            }
            
            
        }
                
        void populate_complicatedpolygon(std::shared_ptr<geometry::ComplicatedPolygon> ele, int64 block_qt, std::vector<std::string>& current) {
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::to_string(-1*ele->Id());
                } else if (col.source == ColumnSource::Part) {
                    //current[i] = std::to_string(ele->Part());
                }else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::to_string(ele->Quadtree());
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::to_string(block_qt);
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::to_string(ele->MinZoom());
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Area) {
                    current[i]=double_string(ele->Area());
                    
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = as_hex(ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = add_tags(ele->Tags(), current);
            } else {
                add_tags(ele->Tags(), current);
            }
            
        }
            
        void populate_complicatedpolygon_part(std::shared_ptr<geometry::ComplicatedPolygon> ele, size_t part, int64 block_qt, std::vector<std::string>& current) {
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::to_string(-1*ele->Id());
                } else if (col.source == ColumnSource::Part) {
                    current[i] = std::to_string(part);
                }else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::to_string(ele->Quadtree());
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::to_string(block_qt);
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::to_string(ele->MinZoom());
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::to_string(ele->ZOrder());
                    }
                } else if (col.source == ColumnSource::Area) {
                    
                    current[i]=double_string(ele->Parts().at(part).area);
                    
                } else if (col.source == ColumnSource::Geometry) {
                    auto w = geometry::polygon_part_wkb(ele->Parts().at(part), true, true);
                    current[i] = as_hex(w);
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = add_tags(ele->Tags(), current);
            } else {
                add_tags(ele->Tags(), current);
            }
            
        }
        
};

std::string pack_pg_int(ColumnType ty, int64 v) {
    if (ty==ColumnType::BigInteger) {
        std::string o(8,'\0');
        write_int64(o, 0, v);
        return o;
    } else if (ty==ColumnType::Integer) {
        std::string o(4,'\0');
        write_int32(o, 0, v);
        return o;
    }
    throw std::domain_error("wrong type?");
    return "";
}
        
std::string pack_pg_double(ColumnType ty, int64 v) {
    if (ty==ColumnType::Double) {
        std::string o(8,'\0');
        write_double(o, 0, v);
        return o;
    }
    throw std::domain_error("wrong type?");
    return "";
}

std::string pack_pgbinary_row(const std::vector<std::pair<bool,std::string>>& fields) {
    size_t tl = 2 + 4*fields.size();
    for (const auto& f: fields) { tl+=f.second.size(); }
    
    size_t pos=0;
    std::string data(tl,'\0');
    
    pos=write_int16(data, pos, fields.size());
    
    
    for (const auto& f: fields) {
        if (f.first==false) {
            //null
            pos = write_int32(data, pos, -1);
        } else {
            pos = write_int32(data, pos, f.second.size());
            pos = _write_data(data, pos, f.second);
        }
    }
    if (tl!=pos) { throw std::domain_error("??"); }
    return data;
}
    


class PackCsvBlocksTableBinary : public PackCsvBlocksTableBase {
    public:
        PackCsvBlocksTableBinary(const TableSpec& table_spec_, bool validate_geometry_)
         : table_spec(table_spec_), validate_geometry(validate_geometry_), othertags_col(-1), has_geometry(false), has_rep_point(false) {
            
            
            for (size_t i=0; i<table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                                
                if (col.source == ColumnSource::OtherTags) {
                    othertags_col=i;
                }
                if (col.source == ColumnSource::Tag) {
                    tag_cols[col.name] = i;
                }
                if (col.source == ColumnSource::Geometry) {
                    has_geometry=true;
                }
                if (col.source == ColumnSource::RepresentativePointGeometry) {
                    has_rep_point=true;
                }
            }
            
            
             
        }
        
        virtual ~PackCsvBlocksTableBinary() {}
        
        std::string header() { throw std::domain_error("not implemeneted"); }
        
        std::string call(ElementPtr ele, int64 block_qt) {
            if (!ele) { throw std::domain_error("??"); }
            
            std::vector<std::pair<bool,std::string>> res(table_spec.columns.size());
            
            if (ele->Type() == ElementType::Point) {
                auto pt = std::dynamic_pointer_cast<geometry::Point>(ele);
                populate_point(pt, block_qt, res);
            } else if (ele->Type() == ElementType::Linestring) {
                auto ln = std::dynamic_pointer_cast<geometry::Linestring>(ele);
                populate_line(ln, block_qt, res);
            } else if (ele->Type() == ElementType::SimplePolygon) {
                auto py = std::dynamic_pointer_cast<geometry::SimplePolygon>(ele);
                populate_simplepolygon(py, block_qt, res);
            } else if (ele->Type() == ElementType::ComplicatedPolygon) {
                auto py = std::dynamic_pointer_cast<geometry::ComplicatedPolygon>(ele);
                populate_complicatedpolygon(py, block_qt, res);
            }
            return pack_pgbinary_row(res);
            
        }
        
        std::string call_complicatedpolygon_part(std::shared_ptr<geometry::ComplicatedPolygon> ele, size_t part, int64 block_qt) {
            if (!ele) { throw std::domain_error("??"); }
            
            std::vector<std::pair<bool,std::string>> res(table_spec.columns.size());
            populate_complicatedpolygon_part(ele, part, block_qt, res);
            return pack_pgbinary_row(res);
        }
        
    
    private:
        TableSpec table_spec;
        bool validate_geometry;
        int othertags_col;
        std::map<std::string,size_t> tag_cols;
        bool has_geometry;
        bool has_rep_point;
        
        
        std::pair<std::string,std::string> prep_geometry(std::shared_ptr<BaseGeometry> geom) {
            if ((!has_geometry) && (!has_rep_point)) {
                return std::make_pair("","");
            }
            if (geom->Type() == oqt::ElementType::Point) {
                auto w = geom->Wkb(true,true);
                return std::make_pair(w,w);
            }
            
            if (has_geometry && (!has_rep_point) && (!validate_geometry)) {
                auto w = geom->Wkb(true,true);
                return std::make_pair(w, "");
            }
            
            auto gg = make_geos_geometry(geom);
            
            if (validate_geometry) {
                gg->validate();
            }
            std::string wkb, ptwkb;
            if (has_geometry) {
                wkb = gg->Wkb();
            }
            if (has_rep_point) {
                ptwkb = gg->PointWkb();
            }
            return std::make_pair(wkb,ptwkb);
        }
        std::pair<std::string,std::string> prep_geometry_cp_part(std::shared_ptr<ComplicatedPolygon> geom, size_t part) {
            if ((!has_geometry) && (!has_rep_point)) {
                return std::make_pair("","");
            }
            
            
            if (has_geometry && (!has_rep_point) && (!validate_geometry)) {
                auto w = geom->Wkb(true,true);
                return std::make_pair(w, "");
            }
            
            auto gg = make_geos_geometry_cp_part(geom,part);
            
            if (validate_geometry) {
                gg->validate();
            }
            std::string wkb, ptwkb;
            if (has_geometry) {
                wkb = gg->Wkb();
            }
            if (has_rep_point) {
                ptwkb = gg->PointWkb();
            }
            return std::make_pair(wkb,ptwkb);
        }
        
        
        
        std::string add_tags(const tagvector& tags, std::vector<std::pair<bool,std::string>>& current) {
            tagvector oo;
            for (const auto& tg: tags) {
                if (tag_cols.count(tg.key)) {
                    current[tag_cols.at(tg.key)] = std::make_pair(true,tg.val);
                } else if (othertags_col>=0) {
                    oo.push_back(tg);
                }
            }
            
            if (othertags_col>=0) {
                return pack_hstoretags_binary(oo);
            }
            return "";
        }
            
            
        
        void populate_point(std::shared_ptr<geometry::Point> ele, int64 block_qt, std::vector<std::pair<bool, std::string>>& current) {
            auto gg = prep_geometry(ele);
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Id()));
                } else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Quadtree()));
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, block_qt));
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>0) {
                        current[i] = std::make_pair(true, pack_pg_int(col.type, ele->MinZoom()));
                    }
                } else if (col.source == ColumnSource::Geometry) {
                    if (!gg.first.empty()) {
                        current[i] = std::make_pair(true, gg.first);
                    }
                } else if (col.source == ColumnSource::RepresentativePointGeometry) {
                    if (!gg.second.empty()) {
                        current[i] = std::make_pair(true, gg.second);
                    }
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
            
        }
        
        void populate_line(std::shared_ptr<geometry::Linestring> ele, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            auto gg = prep_geometry(ele);
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Id()));
                } else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Quadtree()));
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, block_qt));
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::make_pair(true, pack_pg_int(col.type, ele->MinZoom()));
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Length) {
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(ele->Length()*10.0)/10.0));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    if (!gg.first.empty()) {
                        current[i] = std::make_pair(true, gg.first);
                    }
                } else if (col.source == ColumnSource::RepresentativePointGeometry) {
                    if (!gg.second.empty()) {
                        current[i] = std::make_pair(true, gg.second);
                    }
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
        
        }
                
        void populate_simplepolygon(std::shared_ptr<geometry::SimplePolygon> ele, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            auto gg = prep_geometry(ele);
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Id()));
                } else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Quadtree()));
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, block_qt));
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::make_pair(true, pack_pg_int(col.type, ele->MinZoom()));
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Area) {
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(ele->Area()*10.0)/10.0));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    if (!gg.first.empty()) {
                        current[i] = std::make_pair(true, gg.first);
                    }
                } else if (col.source == ColumnSource::RepresentativePointGeometry) {
                    if (!gg.second.empty()) {
                        current[i] = std::make_pair(true, gg.second);
                    }
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
            
        }   
        void populate_complicatedpolygon(std::shared_ptr<geometry::ComplicatedPolygon> ele, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            auto gg = prep_geometry(ele);
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, -1*ele->Id()));
                } else if (col.source == ColumnSource::Part) {
                    //current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Part()));
                }else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Quadtree()));
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, block_qt));
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::make_pair(true, pack_pg_int(col.type, ele->MinZoom()));
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Area) {
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(ele->Area()*10.0)/10.0));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    if (!gg.first.empty()) {
                        current[i] = std::make_pair(true, gg.first);
                    }
                } else if (col.source == ColumnSource::RepresentativePointGeometry) {
                    if (!gg.second.empty()) {
                        current[i] = std::make_pair(true, gg.second);
                    }
                    
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
        }
        
        void populate_complicatedpolygon_part(std::shared_ptr<geometry::ComplicatedPolygon> ele, size_t part, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            auto gg = prep_geometry_cp_part(ele,part);
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, -1*ele->Id()));
                } else if (col.source == ColumnSource::Part) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, part));
                }else if (col.source == ColumnSource::ObjectQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Quadtree()));
                } else if (col.source == ColumnSource::BlockQuadtree) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, block_qt));
                } else if (col.source == ColumnSource::MinZoom) {
                    if (ele->MinZoom()>=0) {
                        current[i] = std::make_pair(true, pack_pg_int(col.type, ele->MinZoom()));
                    }
                } else if (col.source == ColumnSource::ZOrder) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Layer) {
                    if (ele->ZOrder()>0 ) {
                        current[i]=std::make_pair(true, pack_pg_int(col.type, ele->ZOrder()));
                    }
                } else if (col.source == ColumnSource::Area) {
                    double a = ele->Parts().at(part).area;
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(a*10.0)/10.0));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    if (!gg.first.empty()) {
                        current[i] = std::make_pair(true, gg.first);
                    }
                } else if (col.source == ColumnSource::RepresentativePointGeometry) {
                    if (!gg.second.empty()) {
                        current[i] = std::make_pair(true, gg.second);
                    }
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
        }
            
};            
            
            
            
            

class PackCsvBlocksImpl : public PackCsvBlocks {
    public:
        PackCsvBlocksImpl(const PackCsvBlocks::tagspec& tags, bool with_header_, bool binary_format_, table_alloc_func alloc_func_, bool split_multipolygons_, bool validate_geometry_)
            : with_header(with_header_), binary_format(binary_format_),alloc_func(alloc_func_), split_multipolygons(split_multipolygons_),validate_geometry(validate_geometry_) {
            
            if (!alloc_func) {
                alloc_func = default_table_alloc;
            }
            
            for (const auto& ts: tags) {
                if (binary_format) {
                    tables[ts.table_name] = std::make_shared<PackCsvBlocksTableBinary>(ts,validate_geometry);
                } else {
                    tables[ts.table_name] = std::make_shared<PackCsvBlocksTable>(ts);
                }
            }
        }
        
        virtual ~PackCsvBlocksImpl() {}
        
        std::shared_ptr<CsvBlock> call(PrimitiveBlockPtr block) {
            if (!block) { return nullptr; }
            auto res = std::make_shared<CsvBlock>(binary_format);
            
            
                    
            
            for (auto obj: block->Objects()) {
                auto tt = alloc_func(obj);
                for (const auto& tab: tt) {
                    if (tables.count(tab)==0) {
                        if (unknowns.count(tab)==0) {
                            Logger::Message() << "unknown table " << tab;
                        } else {
                            unknowns.insert(tab);
                        }
                        continue;
                    }
                    
                    auto& output = res->get(tab);
                    
                    auto& table = tables.at(tab);
                    
                    if (with_header && (output.size()==0)) {
                        output.add(table->header());
                    }
                    if (split_multipolygons && (obj->Type()==ElementType::ComplicatedPolygon)) {
                        auto cp = std::dynamic_pointer_cast<geometry::ComplicatedPolygon>(obj);
                        if (!cp) {
                            throw std::domain_error("wrong type");
                        }
                        for (size_t i=0; i < cp->Parts().size(); i++) {
                            output.add(table->call_complicatedpolygon_part(cp, i, block->Quadtree()));
                        }
                        
                    } else {
                        output.add(table->call(obj, block->Quadtree()));
                    }
                }
            }           
            
            res->finish();
            return res;
        }
    private:
        std::map<std::string, std::shared_ptr<PackCsvBlocksTableBase>> tables;
        bool with_header;
        bool binary_format;
        table_alloc_func alloc_func;
        bool split_multipolygons;
        bool validate_geometry;
        std::set<std::string> unknowns;
};

std::shared_ptr<PackCsvBlocks> make_pack_csvblocks(const PackCsvBlocks::tagspec& tags, bool with_header, bool binary_format, table_alloc_func alloc_func, bool split_multipolygons, bool validate_geometry) {
    return std::make_shared<PackCsvBlocksImpl>(tags, with_header,binary_format, alloc_func, split_multipolygons,validate_geometry);
}            
            

std::string pack_csv(const CsvRows& rr, const std::string& name) {
    std::list<PbfTag> tt;
    tt.push_back(PbfTag{1,0,name});
    tt.push_back(PbfTag{2,(uint64) rr.size(),""});
    tt.push_back(PbfTag{3,0,rr.data_blob()});
    return pack_pbf_tags(tt);
}

void write_csv_block(std::string outfn, std::shared_ptr<CsvBlock> bl) {
    std::ofstream out(outfn,std::ios::binary);
    
    if (!bl) {
        out.write("EMPTY",5);
        out.close();
        return;
    }
    std::list<PbfTag> ll;
    for (auto& cc: bl->rows()) {
        if (cc.second.size()>0) {
            ll.push_back(PbfTag{1,0,pack_csv(cc.second, cc.first)});
        }
    }
    if (ll.empty()) {
        out.write("EMPTY",5);
        out.close();
        return;
    }
    
    
    auto mm=pack_pbf_tags(ll);
    out.write(mm.data(),mm.size());
    out.close();
}

class PostgisWriterImpl : public PostgisWriter {
    public:
        PostgisWriterImpl(
            const std::string& connection_string_,
            const std::string& table_prfx_,
            bool with_header_, bool as_binary_)
             : connection_string(connection_string_), table_prfx(table_prfx_), with_header(with_header_), as_binary(as_binary_), init(false), ii(0) {
            
            
            
        }
        
        virtual ~PostgisWriterImpl() {}
        
        
        virtual void finish() {
            if (init) {
                auto res = PQexec(conn,"commit");
                PQclear(res);
                PQfinish(conn);
            }
        }
        
        virtual void call(std::shared_ptr<CsvBlock> bl) {
            
            try {
                for (const auto& cc: bl->rows()) {
                    copy_func(table_prfx+cc.first, cc.second.data_blob());        
                }
                
                ii++;
                /*if ((ii % 17731)==0) {
                    auto res = PQexec(conn,"commit");
                    int r = PQresultStatus(res);
                    PQclear(res);
                    if (r!=PGRES_COMMAND_OK){
                        Logger::Message() << "postgiswriter: commit failed " << PQerrorMessage(conn);
                        PQfinish(conn);
                        throw std::domain_error("failed");
                    }
                    res = PQexec(conn,"begin");
                    PQclear(res);
                }*/
            } catch (std::exception& ex) {
                
                write_csv_block("previous.data", prev_block);
                write_csv_block("current.data", bl);
                throw ex;
            }
                
            prev_block=bl;
        }
        
        
        
    private:
        size_t copy_func(const std::string& tab, const std::string& data) {
            if (!init) {
                conn = PQconnectdb(connection_string.c_str());
                if (!conn) {
                    Logger::Message() << "connection to postgresql failed [" << connection_string << "]";
                    throw std::domain_error("connection to postgressql failed");
                }
                auto res = PQexec(conn,"begin");
                if (PQresultStatus(res)!=PGRES_COMMAND_OK) {
                    Logger::Message() << "begin failed?? " <<  PQerrorMessage(conn);
                    PQclear(res);
                    PQfinish(conn);
                    throw std::domain_error("begin failed");
                    return 0;
                }
                PQclear(res);
                init=true;
            }
            
            
            std::string sql="COPY "+tab+" FROM STDIN";
            
            if (as_binary) {
                sql += " (FORMAT binary)";
            } else {
                sql += " csv QUOTE e'\x01' DELIMITER e'\x02'";
                if (with_header) {
                    sql += " HEADER";
                }
            }
            
            auto res = PQexec(conn,sql.c_str());

            if (PQresultStatus(res) != PGRES_COPY_IN) {
                Logger::Message() << "PQresultStatus != PGRES_COPY_IN [" << PQresultStatus(res) << "] " <<  PQerrorMessage(conn);
                Logger::Message() << sql;
                PQclear(res);
                PQfinish(conn);
                init=false;
                throw std::domain_error("PQresultStatus != PGRES_COPY_IN");
                return 0;
            }

            int r = PQputCopyData(conn,data.data(),data.size());
            if (r!=1) {
                Logger::Message() << "copy data failed {r=" << r<< "} [" << sql << "]" << PQerrorMessage(conn) << "\n" ;
                PQputCopyEnd(conn,nullptr);
                PQclear(res);
                return 0;
            }

            

            r = PQputCopyEnd(conn,nullptr);
            if (r!=PGRES_COMMAND_OK) {
                Logger::Message() << "\n*****\ncopy failed [" << sql << "]" << PQerrorMessage(conn) << "\n" ;
                    
                return 0;
            }
            
            PQclear(res);
            
            res = PQgetResult(conn);
            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                Logger::Message() << "copy end failed: " << PQerrorMessage(conn);
                throw std::domain_error("failed");
            }
                            
            PQclear(res);
            return 1;
        }
        std::string connection_string;        
        std::string table_prfx;      
        bool with_header;  
        bool as_binary;
        PGconn* conn;
        bool init;
        size_t ii;
        std::shared_ptr<CsvBlock> prev_block;
};

std::shared_ptr<PostgisWriter> make_postgiswriter(
    const std::string& connection_string,
    const std::string& table_prfx,
    bool with_header, bool as_binary) {
    
    return std::make_shared<PostgisWriterImpl>(connection_string, table_prfx, with_header, as_binary);
}

class CsvBlockCount {
    public:
        CsvBlockCount() {}
        virtual ~CsvBlockCount() {}
        
        void call(std::shared_ptr<CsvBlock> bl) {
            if (!bl) {
                Logger::Message lm;
                lm << "CsvBlockCount finished:";
                for (const auto& tb: counts) {
                    lm << "\n" << tb.first << ": " << tb.second.first << " " << tb.second.second;
                }
                return;
            }
            for (const auto& pp: bl->rows()) {
                counts[pp.first].first += pp.second.size();
                counts[pp.first].second += pp.second.data_blob().size();
            }
        }
        
    private:
        std::map<std::string,std::pair<int64,int64>> counts;
};
            
                
        
std::function<void(std::shared_ptr<CsvBlock>)> make_postgiswriter_callback(
            const std::string& connection_string,
            const std::string& table_prfx,
            bool with_header, bool as_binary) {
    
    if (connection_string=="null") {
        auto cbc=std::make_shared<CsvBlockCount>();
        return [cbc](std::shared_ptr<CsvBlock> bl) { cbc->call(bl); };
    }
    
    auto pw = make_postgiswriter(connection_string,table_prfx, with_header, as_binary);
    return [pw](std::shared_ptr<CsvBlock> bl) {
        if (!bl) {
            Logger::Message() << "PostgisWriter done";
            pw->finish();
        } else {
            pw->call(bl);
        }
    };
}
}}

