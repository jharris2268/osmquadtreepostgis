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

void hstore_quotestring(std::ostream& strm, const std::string& val) {
    strm << '"';
    for (auto c : val) {
        if (c=='\n') {
            //strm << '\\' << 'n';
        } else if (c=='"') {
            strm << '\\' << '"';
        } else if (c=='\t') {
            strm << '\\' << 't';
        } else if (c=='\r') {
            strm << '\\' << 'r';
        } else if (c=='\\') {
            strm << '\\' << '\\';
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

std::string pack_hstoretags(const tagvector& tags) {
    std::stringstream strm;
    
    bool isf=true;
    for (const auto& t: tags) {
        if (!isf) { strm << ", "; }
        hstore_quotestring(strm, t.key);
        
        //strm << picojson::value(t.key).serialize();
        
        strm << "=>";
        hstore_quotestring(strm, t.val);
        //strm << picojson::value(t.val).serialize();
        
        isf=false;
    }
    return strm.str();
}

std::string pack_jsontags_picojson(const tagvector& tags) {
    picojson::object p;
    for (const auto& t: tags) {
        p[t.key] = picojson::value(t.val);
    }
    return picojson::value(p).serialize();
}
    
    

size_t _write_data(std::string& data, size_t pos, const std::string& v) {
    std::copy(v.begin(),v.end(),data.begin()+pos);
    pos+=v.size();
    return pos;
}

std::string pack_hstoretags_binary(const tagvector& tags) {
    size_t len=4 + tags.size()*8;
    for (const auto& tg: tags) {
        len += tg.key.size();
        len += tg.val.size();
    }
    
    std::string out(len,'\0');
    size_t pos=0;
    pos = write_int32(out,pos,tags.size());
    
    for (const auto& tg: tags) {
        pos = write_int32(out,pos,tg.key.size());
        pos = _write_data(out,pos,tg.key);
        pos = write_int32(out,pos,tg.val.size());
        pos = _write_data(out,pos,tg.val);
    }
    return out;
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
                    current[i] = std::to_string(ele->Part());
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
        PackCsvBlocksTableBinary(const TableSpec& table_spec_)
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
    
    private:
        TableSpec table_spec;
        int othertags_col;
        std::map<std::string,size_t> tag_cols;
        
        
        
        
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
                    current[i] = std::make_pair(true, ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
            
        }
        
        void populate_line(std::shared_ptr<geometry::Linestring> ele, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            
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
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(ele->Length()*10)/10));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = std::make_pair(true, ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
        
        }
                
        void populate_simplepolygon(std::shared_ptr<geometry::SimplePolygon> ele, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            
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
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(ele->Area()*10)/10));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = std::make_pair(true, ele->Wkb(true, true));
                }
                
            }
            
            if (othertags_col>=0) {
                current[othertags_col] = std::make_pair(true, add_tags(ele->Tags(), current));
            } else {
                add_tags(ele->Tags(), current);
            }
            
            
        }
                
        void populate_complicatedpolygon(std::shared_ptr<geometry::ComplicatedPolygon> ele, int64 block_qt, std::vector<std::pair<bool,std::string>>& current) {
            
            for (size_t i=0; i < table_spec.columns.size(); i++) {
                const auto& col = table_spec.columns[i];
                if (col.source == ColumnSource::OsmId) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, -1*ele->Id()));
                } else if (col.source == ColumnSource::Part) {
                    current[i] = std::make_pair(true, pack_pg_int(col.type, ele->Part()));
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
                    current[i]=std::make_pair(true, pack_pg_double(col.type, round(ele->Area()*10)/10));
                    
                } else if (col.source == ColumnSource::Geometry) {
                    current[i] = std::make_pair(true, ele->Wkb(true, true));
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
        PackCsvBlocksImpl(const PackCsvBlocks::tagspec& tags, bool with_header_, bool binary_format_, table_alloc_func alloc_func_)
            : with_header(with_header_), binary_format(binary_format_),alloc_func(alloc_func_) {
            
            if (!alloc_func) {
                alloc_func = default_table_alloc;
            }
            
            for (const auto& ts: tags) {
                if (binary_format) {
                    tables[ts.table_name] = std::make_shared<PackCsvBlocksTableBinary>(ts);
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
                    output.add(table->call(obj, block->Quadtree()));
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
        std::set<std::string> unknowns;
};

std::shared_ptr<PackCsvBlocks> make_pack_csvblocks(const PackCsvBlocks::tagspec& tags, bool with_header, bool binary_format, table_alloc_func alloc_func) {
    return std::make_shared<PackCsvBlocksImpl>(tags, with_header,binary_format, alloc_func);
}            
            


/*
class PackCsvBlocksImpl : public PackCsvBlocks {
    public:
        PackCsvBlocksImpl(
            const PackCsvBlocks::tagspec& tags, bool with_header_, bool asjson_) : with_header(with_header_), minzoom(false), other_tags(false), asjson(asjson_),layerint(false) {
                        
            std::stringstream point_ss, line_ss, poly_ss;
            if (with_header) {
                point_ss << "osm_id" << delim << "block_quadtree" << delim << "quadtree" << delim;
                line_ss << "osm_id" << delim << "block_quadtree" << delim << "quadtree" << delim;
                poly_ss << "osm_id" << delim << "part" << delim << "block_quadtree" << delim << "quadtree" << delim;
            }
                
            for (const auto& t: tags) {
                if (std::get<0>(t)=="minzoom") {
                    minzoom=true;
                } else if ( (std::get<0>(t)=="*") || (std::get<0>(t)=="XXX") ){
                    other_tags=true;
                } else if ( (std::get<0>(t)=="layer") ) {
                    layerint = true;
                } else {
                
                    if (std::get<1>(t)) {
                        point_tags.insert(std::make_pair(std::get<0>(t),point_tags.size()));
                        if (with_header) {
                            point_ss << std::get<0>(t) << delim;
                        }
                    }
                    if (std::get<2>(t)) {
                        line_tags.insert(std::make_pair(std::get<0>(t),line_tags.size()));
                        if (with_header) {
                            line_ss << std::get<0>(t) << delim;
                        }
                    }
                    if (std::get<3>(t)) {
                        poly_tags.insert(std::make_pair(std::get<0>(t),poly_tags.size()));
                        if (with_header) {
                            poly_ss << std::get<0>(t) << delim;
                        }
                    }
                }
            }
            
            if (with_header) {
                if (other_tags) {
                    point_ss << "tags" << delim;
                    line_ss << "tags" << delim;
                    poly_ss << "tags" << delim;
                }
                if (layerint) {
                    point_ss << "layer" << delim;
                    line_ss << "layer" << delim;
                    poly_ss << "layer" << delim;
                }
                
                
                if (minzoom) {
                    point_ss << "minzoom" << delim;
                    line_ss << "minzoom" << delim;
                    poly_ss << "minzoom" << delim;
                }
                line_ss << "z_order" << delim;
                poly_ss << "z_order" << delim;
                
                line_ss << "length" << delim;
                poly_ss << "way_area" << delim;
                
                point_ss << "way\n";
                line_ss << "way\n";
                poly_ss << "way\n";
                
            
                point_header = point_ss.str();
                line_header = line_ss.str();
                poly_header = poly_ss.str();
            }
            
        }

        virtual ~PackCsvBlocksImpl() {}

        void add_to_csvblock(PrimitiveBlockPtr bl, std::shared_ptr<CsvBlock> res) {
            if (bl->size()==0) {
                return;
            }
            for (auto o : bl->Objects()) {

                if (o->Type()==ElementType::Point) {
                    std::stringstream ss;
                    ss << std::dec << o->Id() << delim << bl->Quadtree() << delim << o->Quadtree() << delim;
                    auto pt = std::dynamic_pointer_cast<Point>(o);
                    auto other = prep_tags(ss,point_tags,o, other_tags, asjson);
                    
                    
                    if (other_tags) {
                        if (!other.empty()) {
                            ss << quote << other << quote;
                        }
                        ss <<delim;
                    }
                    if (layerint) {
                        if (pt->Layer()!=0) {
                            ss << pt->Layer();
                        }
                        ss<<delim;
                    }
                    
                    if (minzoom) {
                        if ((pt->MinZoom()>=0) && (pt->MinZoom()<100)) {
                            ss << std::dec << pt->MinZoom();
                        }
                        ss << delim;
                    }
                    
                    ss << as_hex(pt->Wkb(true, true));
                    ss << "\n";
                    if (with_header && (res->points.size()==0)) {
                        res->points.add(point_header);
                    }
                    
                    res->points.add(ss.str());

                } else if (o->Type()==ElementType::Linestring) {
                    std::stringstream ss;
                    ss << std::dec << o->Id() << delim << bl->Quadtree() << delim << o->Quadtree() << delim;
                    auto ln = std::dynamic_pointer_cast<Linestring>(o);
                    auto other = prep_tags(ss,line_tags,o, other_tags, asjson);
                    
                    if (other_tags) {
                        if (!other.empty()) {
                            ss << quote << other << quote;
                        }
                        ss << delim;
                    }
                    if (layerint) {
                        if (ln->Layer()!=0) {
                            ss << ln->Layer();
                        }
                        ss<<delim;
                    }
                    
                    if (minzoom) {
                        if ((ln->MinZoom()>=0) && (ln->MinZoom()<100)) {
                            ss << std::dec << ln->MinZoom();
                        }
                        ss << delim;
                    }
                    ss << std::dec << ln->ZOrder() << delim;
                    ss << std::fixed << std::setprecision(1) << ln->Length() << delim;
                    ss << as_hex(ln->Wkb(true, true));
                    ss << "\n";
                    if (with_header && (res->lines.size()==0)) {
                        res->lines.add(point_header);
                    }
                    res->lines.add(ss.str());
                } else if (o->Type()==ElementType::SimplePolygon) {
                    std::stringstream ss;
                    ss << std::dec << o->Id() << delim << delim << bl->Quadtree() << delim << o->Quadtree() << delim;
                    
                    auto py = std::dynamic_pointer_cast<SimplePolygon>(o);
                    auto other = prep_tags(ss,poly_tags,o, other_tags, asjson);
                    
                    if (other_tags) {
                        if (!other.empty()) {
                            ss << quote << other << quote;
                        }
                        ss << delim;
                    }
                    if (layerint) {
                        if (py->Layer()!=0) {
                            ss << py->Layer();
                        }
                        ss<<delim;
                    }
                    
                    if (minzoom) {
                        if ((py->MinZoom()>=0) && (py->MinZoom()<100)) {
                            ss << std::dec << py->MinZoom();
                        }
                        ss << delim;
                    }
                    ss << std::dec << py->ZOrder() << delim << std::fixed << std::setprecision(1) << py->Area() << delim;
                    ss << as_hex(py->Wkb(true, true));
                    ss << "\n";
                    if (with_header && (res->polygons.size()==0)) {
                        res->polygons.add(point_header);
                    }
                    res->polygons.add(ss.str());
                } else if (o->Type()==ElementType::ComplicatedPolygon) {
                    std::stringstream ss;
                    auto py = std::dynamic_pointer_cast<ComplicatedPolygon>(o);
                    ss << std::dec << (-1ll * o->Id()) << delim << py->Part() << delim << bl->Quadtree() << delim << o->Quadtree() << delim;
                    auto other = prep_tags(ss,poly_tags,o, other_tags, asjson);
                    if (other_tags) {
                        if (!other.empty()) {
                            ss << quote << other << quote;
                        }
                        ss << delim;
                    }
                    if (layerint) {
                        if (py->Layer()!=0) {
                            ss << py->Layer();
                        }
                        ss<<delim;
                    }
                    
                    if (minzoom) {
                        if ((py->MinZoom()>=0) && (py->MinZoom()<100)) {
                            ss << std::dec << py->MinZoom();
                        }
                        ss << delim;
                    }
                    ss << std::dec << py->ZOrder() << delim << std::fixed << std::setprecision(1) << py->Area() << delim;
                    ss << as_hex(py->Wkb(true, true));
                    ss << "\n";
                    if (with_header && (res->polygons.size()==0)) {
                        res->polygons.add(point_header);
                    }
                    res->polygons.add(ss.str());
                }
            }
        }
       
        
        std::shared_ptr<CsvBlock> call(PrimitiveBlockPtr block) {
            if (!block) { return nullptr; }
            auto res = std::make_shared<CsvBlock>(false);
            add_to_csvblock(block,res);
            res->points.finish();
            res->lines.finish();
            res->polygons.finish();
            return res;
        }
        

    private:

        std::map<std::string,size_t> point_tags;
        std::map<std::string,size_t> line_tags;
        std::map<std::string,size_t> poly_tags;
        bool with_header;
        bool minzoom;
        bool other_tags;
        bool asjson;
        bool layerint;
        std::string point_header;
        std::string line_header;
        std::string poly_header;
};



class PackCsvBlocksBinaryImpl : public PackCsvBlocks {
    public:
        PackCsvBlocksBinaryImpl(
            const PackCsvBlocks::tagspec& tags) : minzoom(false), other_tags(false),layerint(false) {
                
             
            for (const auto& t: tags) {
                if (std::get<0>(t)=="minzoom") {
                    minzoom=true;
                } else if ( (std::get<0>(t)=="*") || (std::get<0>(t)=="XXX") ){
                    other_tags=true;
                } else if ( (std::get<0>(t)=="layer") ) {
                    layerint = true;
                } else {
                
                    if (std::get<1>(t)) {
                        point_tags.insert(std::make_pair(std::get<0>(t),point_tags.size()));
                        
                    }
                    if (std::get<2>(t)) {
                        line_tags.insert(std::make_pair(std::get<0>(t),line_tags.size()));
                        
                    }
                    if (std::get<3>(t)) {
                        poly_tags.insert(std::make_pair(std::get<0>(t),poly_tags.size()));
                        
                    }
                }
            }
            
            
            
        }

        virtual ~PackCsvBlocksBinaryImpl() {}

        
        std::string pack_point(std::shared_ptr<Point> pt, int64 tileqt) {
            std::string header(38,'\0');
            size_t pos=0;
            
            size_t numfields=3 + point_tags.size() + (other_tags?1:0)+(layerint?1:0)+(minzoom?1:0)+ 1;
            pos=write_int16(header, pos, numfields);

            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,pt->Id());
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,tileqt);
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,pt->Quadtree());
            
            std::string tags,othertags;
            std::tie(tags,othertags) = prep_tags_binary(point_tags, pt, other_tags);
            
            std::string wkb=pt->Wkb(true,true);
            
            std::string tail(4+othertags.size()+8+8+4, '\0');
            
            pos=0;
            if (other_tags) {
                if (othertags.size()>0) {
                    pos=write_int32(tail,pos,othertags.size());
                    pos=_write_data(tail,pos,othertags);
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            if (layerint) {
                if (pt->Layer()!=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,pt->Layer());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            if (minzoom) {
                if (pt->MinZoom()>=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,pt->MinZoom());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            pos=write_int32(tail,pos,wkb.size());
            tail.resize(pos);
            
            return header + tags + tail + wkb;
        }
        
        std::string pack_linestring(std::shared_ptr<Linestring> ln, int64 tileqt) {
            std::string header(38,'\0');
            size_t pos=0;
            
            size_t numfields=3 + line_tags.size() + (other_tags?1:0)+(layerint?1:0)+(minzoom?1:0)+ 3;
            pos=write_int16(header, pos, numfields);

            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,ln->Id());
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,tileqt);
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,ln->Quadtree());
            
            std::string tags,othertags;
            std::tie(tags,othertags) = prep_tags_binary(line_tags, ln, other_tags);
            
            std::string wkb=ln->Wkb(true,true);
            
            std::string tail(4+othertags.size()+8+8+4+8+12, '\0');
            
            pos=0;
            if (other_tags) {
                if (othertags.size()>0) {
                    pos=write_int32(tail,pos,othertags.size());
                    pos=_write_data(tail,pos,othertags);
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            if (layerint) {
                if (ln->Layer()!=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,ln->Layer());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            if (minzoom) {
                if (ln->MinZoom()>=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,ln->MinZoom());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            pos=write_int32(tail,pos,4);
            pos=write_int32(tail,pos,ln->ZOrder());
            
            pos=write_int32(tail,pos,8);
            pos=write_double(tail,pos,round(ln->Length()*10)/10);
            
            pos=write_int32(tail,pos,wkb.size());
            tail.resize(pos);
            
            return header + tags + tail + wkb;
        }
        
        std::string pack_simplepolygon(std::shared_ptr<SimplePolygon> py, int64 tileqt) {
            std::string header(42,'\0');
            size_t pos=0;
            
            size_t numfields=3 + poly_tags.size() + (other_tags?1:0)+(layerint?1:0)+(minzoom?1:0)+ 4;
            pos=write_int16(header, pos, numfields);

            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,py->Id());
            
            pos=write_int32(header,pos,-1);
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,tileqt);
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,py->Quadtree());
            
            std::string tags,othertags;
            std::tie(tags,othertags) = prep_tags_binary(poly_tags, py, other_tags);
            
            std::string wkb=py->Wkb(true,true);
            
            std::string tail(4+othertags.size()+8+8+4+8+12, '\0');
            
            pos=0;
            if (other_tags) {
                if (othertags.size()>0) {
                    pos=write_int32(tail,pos,othertags.size());
                    pos=_write_data(tail,pos,othertags);
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            if (layerint) {
                if (py->Layer()!=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,py->Layer());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            if (minzoom) {
                if (py->MinZoom()>=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,py->MinZoom());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            pos=write_int32(tail,pos,4);
            pos=write_int32(tail,pos,py->ZOrder());
            
            pos=write_int32(tail,pos,8);
            pos=write_double(tail,pos,round(10*py->Area())/10.0);
            
            pos=write_int32(tail,pos,wkb.size());
            tail.resize(pos);
            
            return header + tags + tail + wkb;
        }
        
        std::string pack_complicatedpolygon(std::shared_ptr<ComplicatedPolygon> py, int64 tileqt) {
            std::string header(46,'\0');
            size_t pos=0;
            
            size_t numfields=3 + poly_tags.size() + (other_tags?1:0)+(layerint?1:0)+(minzoom?1:0)+ 4;
            pos=write_int16(header, pos, numfields);

            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,py->Id()*-1);
            
            pos=write_int32(header,pos,4);
            pos=write_int32(header,pos,py->Part());
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,tileqt);
            
            pos=write_int32(header,pos,8);
            pos=write_int64(header,pos,py->Quadtree());
            
            std::string tags,othertags;
            std::tie(tags,othertags) = prep_tags_binary(poly_tags, py, other_tags);
            
            std::string wkb=py->Wkb(true,true);
            
            std::string tail(4+othertags.size()+8+8+4+8+12, '\0');
            
            pos=0;
            if (other_tags) {
                if (othertags.size()>0) {
                    pos=write_int32(tail,pos,othertags.size());
                    pos=_write_data(tail,pos,othertags);
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            if (layerint) {
                if (py->Layer()!=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,py->Layer());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            if (minzoom) {
                if (py->MinZoom()>=0) {
                    pos=write_int32(tail,pos,4);
                    pos=write_int32(tail,pos,py->MinZoom());
                } else {
                    pos=write_int32(tail,pos,-1);
                }
            }
            
            pos=write_int32(tail,pos,4);
            pos=write_int32(tail,pos,py->ZOrder());
            
            pos=write_int32(tail,pos,8);
            pos=write_double(tail,pos,round(10*py->Area())/10.0);
            
            pos=write_int32(tail,pos,wkb.size());
            tail.resize(pos);
            
            return header + tags + tail + wkb;
        }

        void add_to_csvblock(PrimitiveBlockPtr bl, std::shared_ptr<CsvBlock> res) {
            if (bl->size()==0) {
                return;
            }
            for (auto o : bl->Objects()) {

                if (o->Type()==ElementType::Point) {
                    
                    auto pt=std::dynamic_pointer_cast<Point>(o);
                    res->points.add(pack_point(pt,bl->Quadtree()));

                } else if (o->Type()==ElementType::Linestring) {
                    auto ln = std::dynamic_pointer_cast<Linestring>(o);
                    res->lines.add(pack_linestring(ln,bl->Quadtree()));
                } else if (o->Type()==ElementType::SimplePolygon) {
                    auto py = std::dynamic_pointer_cast<SimplePolygon>(o);
                    res->polygons.add(pack_simplepolygon(py,bl->Quadtree()));
                } else if (o->Type()==ElementType::ComplicatedPolygon) {
                    auto py = std::dynamic_pointer_cast<ComplicatedPolygon>(o);
                    res->polygons.add(pack_complicatedpolygon(py,bl->Quadtree()));
                }
            }
        }
       
        
        std::shared_ptr<CsvBlock> call(PrimitiveBlockPtr block) {
            if (!block) { return nullptr; }
            auto res = std::make_shared<CsvBlock>(true);
            add_to_csvblock(block,res);
            res->points.finish();
            res->lines.finish();
            res->polygons.finish();
            return res;
        }
        

    private:

        std::map<std::string,size_t> point_tags;
        std::map<std::string,size_t> line_tags;
        std::map<std::string,size_t> poly_tags;
        bool with_header;
        bool minzoom;
        bool other_tags;
        bool asjson;
        bool layerint;
        std::string point_header;
        std::string line_header;
        std::string poly_header;
};

std::shared_ptr<PackCsvBlocks> make_pack_csvblocks(const PackCsvBlocks::tagspec& tags, bool with_header, bool binary_format) {
    if (binary_format) {
        //throw std::domain_error("not implmented");
        return std::make_shared<PackCsvBlocksBinaryImpl>(tags);
    }
    return std::make_shared<PackCsvBlocksImpl>(tags, with_header,false);
}

*/
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
            PQclear(res);
            if (r!=PGRES_COMMAND_OK) {
                Logger::Message() << "\n*****\ncopy failed [" << sql << "]" << PQerrorMessage(conn) << "\n" ;
                    
                return 0;
            }
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

