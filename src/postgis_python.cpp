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


#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>


#include "oqt/elements/block.hpp"
#include "oqt/utils/logger.hpp"

#include <algorithm>
#include <memory>
#include <tuple>


#include "oqt/utils/splitcallback.hpp"

#include "oqt/geometry/process.hpp"
#include "processpostgis.hpp"
#include "postgiswriter.hpp"

#include "oqt/geometry/elements/ring.hpp"
#include "oqt/geometry/elements/point.hpp"
#include "oqt/geometry/elements/linestring.hpp"
#include "oqt/geometry/elements/simplepolygon.hpp"
#include "oqt/geometry/elements/complicatedpolygon.hpp"
#include "oqt/geometry/elements/waywithnodes.hpp"
#include "gzstream.hpp"

#include "validategeoms.hpp"
#include <cmath> 
using namespace oqt;


namespace py = pybind11;


template <class RetType, class ArgType>
std::function<RetType(ArgType)> wrap_callback(std::function<RetType(ArgType)> callback) {
    if (!callback) {
        return nullptr;
    }
    return [callback](ArgType arg) {
        py::gil_scoped_acquire aq;
        return callback(arg);
    };
}

template <class BlockType>
class collect_blocks {
    public:
        collect_blocks(
            std::function<bool(std::vector<std::shared_ptr<BlockType>>)> callback_,
            size_t numblocks_) : callback(callback_), numblocks(numblocks_), tot(0) {}

        void call(std::shared_ptr<BlockType> block) {
            
            if (block) {
                pending.push_back(block);
            }
            if ((pending.size() == numblocks) || (!block && !pending.empty())) {
                std::vector<std::shared_ptr<BlockType>> tosend;
                tosend.swap(pending);
                tot+=tosend.size();
                
                if (!callback) {
                    oqt::Logger::Message() << "None(" << tosend.size() << " blocks" << ")";
                } else {
                        
                    bool r = callback(tosend);
                    if (!r) {
                        throw std::domain_error("python callback failed");
                    }
                }

            }
        }

        size_t total() { return tot; }

    private:
        std::function<bool(std::vector<std::shared_ptr<BlockType>>)> callback;
        size_t numblocks;
        size_t tot;
        std::vector<std::shared_ptr<BlockType>> pending;
};

typedef std::function<bool(std::vector<oqt::PrimitiveBlockPtr>)> external_callback;
typedef std::function<void(oqt::PrimitiveBlockPtr)> block_callback;

inline block_callback prep_callback(external_callback cb, size_t numblocks) {
    if (!cb) { return nullptr; }
    
    auto collect = std::make_shared<collect_blocks<oqt::PrimitiveBlock>>(wrap_callback(cb),numblocks);
    return [collect](oqt::PrimitiveBlockPtr bl) { collect->call(bl); };
}

geometry::mperrorvec process_geometry_postgis_py(const geometry::GeometryParameters& params, const geometry::PostgisParameters& postgis, external_callback cb) {
    
    py::gil_scoped_release r;
    block_callback wrapped = prep_callback(cb, params.numblocks);
    
    return process_geometry_postgis(params, postgis, wrapped);
}
    
geometry::mperrorvec process_geometry_postgis_nothread_py(const geometry::GeometryParameters& params, const geometry::PostgisParameters& postgis, external_callback cb) {
    
    py::gil_scoped_release r;
    block_callback wrapped = prep_callback(cb, params.numblocks);
    
    return process_geometry_postgis_nothread(params, postgis, wrapped);
}


geometry::mperrorvec process_geometry_csvcallback_py(const geometry::GeometryParameters& params,
    const geometry::PostgisParameters& postgis, 
    external_callback cb,
    std::function<void(std::shared_ptr<geometry::CsvBlock>)> csvblock_callback) {

    py::gil_scoped_release r;
    
    block_callback wrapped = prep_callback(cb, params.numblocks);
    
    return process_geometry_csvcallback(params, postgis, wrapped, wrap_callback(csvblock_callback));
}

geometry::mperrorvec process_geometry_csvcallback_nothread_py(const geometry::GeometryParameters& params,
    const geometry::PostgisParameters& postgis, 
    external_callback cb,
    std::function<void(std::shared_ptr<geometry::CsvBlock>)> csvblock_callback) {

    py::gil_scoped_release r;
    
    block_callback wrapped = prep_callback(cb, params.numblocks);
    
    return process_geometry_csvcallback_nothread(params, postgis, wrapped, wrap_callback(csvblock_callback));
}









class CsvWriter {
    public:
        CsvWriter(const std::string& prfx_) : prfx(prfx_) {}
        
        void call(std::shared_ptr<oqt::geometry::CsvBlock> block) {
            if (!block) {
                for (auto& oo: outs) {
                    oo.second->close();
                }
                return;
            }
            for (const auto& r: block->rows()) {
                bool hh=false;
                if (outs.count(r.first)==0) {
                    hh=true;
                    std::string fn(prfx+"-"+r.first+".csv.gz");
                    outs.emplace(r.first, std::make_shared<gzstream::ogzstream>(fn.c_str()));
                }
                
                gzstream::ogzstream& out = *outs.at(r.first);
                
                if (hh) {
                    out << r.second.at(0);
                }
                
                for (int i=1; i < r.second.size(); i++) {
                    out << r.second.at(i);
                }
                
            }
            
        }
    private:
        std::string prfx;
        std::map<std::string,std::shared_ptr<gzstream::ogzstream>> outs;
};

geometry::mperrorvec process_geometry_csvcallback_write(const geometry::GeometryParameters& params,
    const geometry::PostgisParameters& postgis,
    external_callback cb,
    const std::string& out_prfx) {

    py::gil_scoped_release r;
    
    block_callback wrapped = prep_callback(cb, params.numblocks);
    
    auto writer=std::make_shared<CsvWriter>(out_prfx);
    auto csvblock_callback = [writer](std::shared_ptr<oqt::geometry::CsvBlock> bl) { writer->call(bl); };
    
    return process_geometry_csvcallback(params, postgis, wrapped, csvblock_callback);
}
std::vector<std::string> extended_table_alloc(ElementPtr geom) {
    if (geom->Type()==ElementType::Point) {
        return {"point"};
    }
    
    if (geom->Type()==ElementType::Linestring) {
        auto ln = std::dynamic_pointer_cast<geometry::Linestring>(geom);
        if (ln->ZOrder() > 0) {
            return {"highway"};
        } else {
            return {"line"};
        }
    }   
    
    if (geom->Type()==ElementType::SimplePolygon) {
        bool is_building=false;
        for (const auto& tg: geom->Tags()) {
            if ((tg.key=="building") && (tg.val != "no")) {
                is_building=true;
            }
        }
        if (is_building) {
            return {"building"};
        }
        return {"polygon"};
        
    }   
    
    if (geom->Type() == ElementType::ComplicatedPolygon) {
        bool is_boundary=false;
        bool is_building=false;
        
        for (const auto& tg: geom->Tags()) {
            if ((tg.key=="type") && (tg.val == "boundary")) {
                is_boundary=true;
            }
            if ((tg.key=="building") && (tg.val != "no")) {
                is_building=true;
            }
        }
        if (is_boundary) {
            return {"polygon","boundary"};
        }
        if (is_building) {
            return {"building"};
        }
        return {"polygon"};
        
    }   
    return {};
}

void set_params_alloc_func(geometry::PostgisParameters& gp, py::object obj) {
    if (obj.is_none()) {
        gp.alloc_func =  geometry::default_table_alloc;
        return;
    }
    try {
        auto s = py::cast<std::string>(obj);
        if (s=="default") {
            gp.alloc_func = geometry::default_table_alloc;
            return;
        }
        if (s=="extended") {
            gp.alloc_func = extended_table_alloc;
            return;
        }
    } catch (...) {}
    try {
        auto p = py::cast<geometry::table_alloc_func>(obj);
        gp.alloc_func = [p](ElementPtr e) {
            py::gil_scoped_acquire g;
            return p(e);
        };
        return;
    } catch (...) {}
    throw std::domain_error("can't set alloc func");
}


    
    


PYBIND11_DECLARE_HOLDER_TYPE(XX, std::shared_ptr<XX>);
void osmquadtreepostgis_defs(py::module& m) {
    
    
    py::class_<geometry::CsvBlock, std::shared_ptr<geometry::CsvBlock>>(m,"CsvBlock")
        .def_property_readonly("rows", &geometry::CsvBlock::rows);
    ;
    py::class_<geometry::CsvRows>(m, "CsvRows")
        .def("__getitem__", &geometry::CsvRows::at)
        .def("__len__", &geometry::CsvRows::size)
        .def("data", [](const geometry::CsvRows& c) { return py::bytes(c.data_blob()); })
    ;
    py::class_<geometry::PostgisParameters>(m, "PostgisParameters")
        .def(py::init<>())
        .def_readwrite("connstring", &geometry::PostgisParameters::connstring)
        .def_readwrite("tableprfx", &geometry::PostgisParameters::tableprfx)
        .def_readwrite("coltags", &geometry::PostgisParameters::coltags)
        .def_readwrite("use_binary", &geometry::PostgisParameters::use_binary)
        .def_property("alloc_func", [](geometry::PostgisParameters& gp) { return gp.alloc_func; }, &set_params_alloc_func) 
        .def_readwrite("split_multipolygons", &geometry::PostgisParameters::split_multipolygons)
        .def_readwrite("validate_geometry", &geometry::PostgisParameters::validate_geometry)
        .def_readwrite("round_geometry", &geometry::PostgisParameters::round_geometry)
    ;
    
    m.def("process_geometry_postgis", &process_geometry_postgis_py);
    m.def("process_geometry_postgis_nothread", &process_geometry_postgis_nothread_py);
    
    m.def("process_geometry_csvcallback_nothread", &process_geometry_csvcallback_nothread_py);
    m.def("process_geometry_csvcallback", &process_geometry_csvcallback_py);
    m.def("process_geometry_csvcallback_write", &process_geometry_csvcallback_write);
    
    
    py::class_<geometry::PostgisWriter, std::shared_ptr<geometry::PostgisWriter>>(m, "PostgisWriter")
        .def("finish", &geometry::PostgisWriter::finish)
        .def("call", &geometry::PostgisWriter::call)
    ;
    m.def("make_postgiswriter", &geometry::make_postgiswriter);
    
    py::class_<geometry::PackCsvBlocks, std::shared_ptr<geometry::PackCsvBlocks>>(m, "PackCsvBlocks")
        .def("call", &geometry::PackCsvBlocks::call)
    ;
    m.def("make_pack_csvblocks", &geometry::make_pack_csvblocks);
    m.def("extended_table_alloc", &extended_table_alloc);
    m.def("pack_hstoretags", &geometry::pack_hstoretags);
    m.def("pack_hstoretags_binary", &geometry::pack_hstoretags_binary);
    m.def("pack_jsontags_picojson", &geometry::pack_jsontags_picojson);
    
    
    py::enum_<geometry::ColumnType>(m, "GeometryColumnType")
        .value("Text", geometry::ColumnType::Text)
        .value("BigInteger", geometry::ColumnType::BigInteger)
        .value("Integer", geometry::ColumnType::Integer)
        .value("Double",geometry::ColumnType::Double)
        .value("Hstore", geometry::ColumnType::Hstore)
        .value("Json", geometry::ColumnType::Json)
        .value("TextArray", geometry::ColumnType::TextArray)
        .value("Geometry", geometry::ColumnType::Geometry)
        .value("PointGeometry", geometry::ColumnType::PointGeometry)
        .value("LineGeometry", geometry::ColumnType::LineGeometry)
        .value("PolygonGeometry", geometry::ColumnType::PolygonGeometry)
    ;
    py::enum_<geometry::ColumnSource>(m, "GeometryColumnSource")
        .value("OsmId", geometry::ColumnSource::OsmId)
        .value("Part", geometry::ColumnSource::Part)
        .value("ObjectQuadtree", geometry::ColumnSource::ObjectQuadtree)
        .value("BlockQuadtree", geometry::ColumnSource::BlockQuadtree)
        .value("Tag", geometry::ColumnSource::Tag)
        .value("OtherTags", geometry::ColumnSource::OtherTags)
        .value("Layer", geometry::ColumnSource::Layer)
        .value("ZOrder", geometry::ColumnSource::ZOrder)
        .value("MinZoom", geometry::ColumnSource::MinZoom)
        .value("Length", geometry::ColumnSource::Length)
        .value("Area", geometry::ColumnSource::Area)
        .value("Geometry", geometry::ColumnSource::Geometry)
        .value("RepresentativePointGeometry", geometry::ColumnSource::RepresentativePointGeometry)
        .value("BoundaryLineGeometry", geometry::ColumnSource::BoundaryLineGeometry)
    ;
    
    py::class_<geometry::ColumnSpec>(m, "GeometryColumnSpec")
        .def(py::init<std::string,geometry::ColumnType,geometry::ColumnSource>())
        .def_readwrite("name", &geometry::ColumnSpec::name)
        .def_readwrite("type", &geometry::ColumnSpec::type)
        .def_readwrite("source", &geometry::ColumnSpec::source)
    ;
    
    py::class_<geometry::TableSpec>(m, "GeometryTableSpec")
        .def(py::init<std::string>())
        .def_readwrite("table_name", &geometry::TableSpec::table_name)
        .def_readonly("columns", &geometry::TableSpec::columns)
        .def("set_columns", [](geometry::TableSpec& ts, const std::vector<geometry::ColumnSpec>& cc) {
            ts.columns=cc;
        })
    ;
    m.def("validate_geometry", [](std::shared_ptr<BaseGeometry> ele, bool round_geometry) {
        auto gg = geometry::make_geos_geometry(ele,round_geometry);
        gg->validate();
        auto a = py::bytes(gg->Wkb());
        auto b = py::bytes(gg->PointWkb());
        return py::make_tuple(a,b);
    });
    
};

PYBIND11_MODULE(_osmquadtreepostgis, m) {
    m.doc() = "pybind11 example plugin";
    osmquadtreepostgis_defs(m);
    
}

