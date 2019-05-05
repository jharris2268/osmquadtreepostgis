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

#include "validategeoms.hpp"
#include <oqt/utils/pbf/fixedint.hpp>
#include "geos_c.h"

namespace oqt {
namespace geometry {
    
    
class GeosGeometryImpl : public GeosGeometry {
    public:
        GeosGeometryImpl(std::shared_ptr<oqt::BaseGeometry> geom) {
            
            handle = GEOS_init_r();
            
            
            
            if (geom->Type() == oqt::ElementType::Point) {
                geometry = make_point(std::dynamic_pointer_cast<Point>(geom));
            } else if (geom->Type() == oqt::ElementType::Linestring) {
                geometry = make_linestring(std::dynamic_pointer_cast<Linestring>(geom));
            }  else if (geom->Type() == oqt::ElementType::SimplePolygon) {
                geometry = make_simplepolygon(std::dynamic_pointer_cast<SimplePolygon>(geom));
            }  else if (geom->Type() == oqt::ElementType::ComplicatedPolygon) {
                geometry = make_complicatedpolygon(std::dynamic_pointer_cast<ComplicatedPolygon>(geom));
            } else {
                geometry = GEOSGeom_createEmptyCollection_r(handle, 7);
            }
        }
        
        GeosGeometryImpl(std::shared_ptr<ComplicatedPolygon> geom, size_t part) {
            handle = GEOS_init_r();
            geometry = make_complicatedpolygon_part(geom->Parts().at(part));
        }
        
        virtual ~GeosGeometryImpl() {
            if (geometry) {
                GEOSGeom_destroy_r(handle, geometry);
            }
            GEOS_finish_r(handle);
        };
        
        void validate() {
            
            //MakeValid not present yet in released versions of libgeos [May 2019]
            //GEOSGeometry* result = GEOSMakeValid_r(handle, geometry);
            
            
            
            int t = GEOSGeomTypeId_r(handle,geometry);
            if ((t==3) || (t==6)) {
                if (!GEOSisValid_r(handle, geometry)) {
                    GEOSGeometry* result = GEOSBuffer_r(handle, geometry, 0, 16);
            
                    if (result) {
                        GEOSGeom_destroy_r(handle, geometry);
                        geometry=result;
                    }
                }
            }
        }
        void simplify(double tol) {
            GEOSGeometry* result = GEOSTopologyPreserveSimplify_r(handle, geometry, tol);
            if (result) {
                GEOSGeom_destroy_r(handle, geometry);
                geometry=result;
            }
        }
        
        
        std::string Wkb() {
            return write_wkb(geometry);
        }
        
        
        std::string PointWkb() {
            GEOSGeometry* point = GEOSPointOnSurface_r(handle, geometry);
            auto wkb= write_wkb(point);
            GEOSGeom_destroy_r(handle, point);
            return wkb;
        }
    
    private:
        GEOSContextHandle_t handle;
        GEOSGeometry* geometry;
        
        
        std::string write_wkb(GEOSGeometry* geom) {
            //GEOS_setWKBByteOrder_r(handle, GEOS_WKB_XDR);
            GEOSSetSRID_r(handle, geom, 3857);
            
            GEOSWKBWriter* writer = GEOSWKBWriter_create_r(handle);
            GEOSWKBWriter_setIncludeSRID_r(handle, writer, 1);
            GEOSWKBWriter_setByteOrder_r(handle, writer, GEOS_WKB_XDR);
            
            
            std::string s;            
            size_t sz;
            unsigned char* c = GEOSWKBWriter_write_r(handle, writer, geom, &sz);
            
            
            if (c) {
                s = std::string(reinterpret_cast<const char*>(c), sz);
                GEOSFree_r(handle, c);
            }
            GEOSWKBWriter_destroy_r(handle, writer);
            return s;
            
            
        }
        
        
        GEOSGeometry* make_point(std::shared_ptr<Point> pt) {
            
            
            GEOSCoordSequence* coords = make_coords({pt->LonLat()});
            return GEOSGeom_createPoint_r(handle, coords);
        }
        
        GEOSCoordSequence* make_coords(const std::vector<LonLat>& lls) {
            
            GEOSCoordSequence* coords = GEOSCoordSeq_create_r(handle, lls.size(), 2);
            for (size_t i=0; i < lls.size(); i++) {
                const auto& ll = lls[i];
                auto p = forward_transform(ll.lon, ll.lat);
                GEOSCoordSeq_setX_r(handle, coords, i, p.x);
                GEOSCoordSeq_setY_r(handle, coords, i, p.y);
            }
            return coords;
        }
        
        GEOSGeometry* make_linestring(std::shared_ptr<Linestring> line) {
            GEOSCoordSequence* coords = make_coords(line->LonLats());
            return GEOSGeom_createLineString_r(handle, coords);
        }
        
        GEOSGeometry* make_simplepolygon(std::shared_ptr<SimplePolygon> line) {
            GEOSGeometry* outer = GEOSGeom_createLinearRing_r(handle, make_coords(line->LonLats()));
            return GEOSGeom_createPolygon_r(handle, outer, nullptr, 0);
        }
        
        GEOSGeometry* make_complicatedpolygon_part(const PolygonPart& part) {
            GEOSGeometry* outer = GEOSGeom_createLinearRing_r(handle, make_coords(ringpart_lonlats(part.outer)));
            if (part.inners.empty()) {
                return GEOSGeom_createPolygon_r(handle, outer, nullptr, 0);
            }
            
            std::vector<GEOSGeometry*> inners;
            for (const auto& inn: part.inners) {
                inners.push_back(GEOSGeom_createLinearRing_r(handle, make_coords(ringpart_lonlats(inn))));
            }
            
            return GEOSGeom_createPolygon_r(handle, outer, &inners[0], inners.size());
        }
        
        GEOSGeometry* make_complicatedpolygon(std::shared_ptr<ComplicatedPolygon> poly) {
            
            if (poly->Parts().size()==0) {
                return GEOSGeom_createEmptyCollection_r(handle, 7);
            } else if (poly->Parts().size()==1) {
                
                return make_complicatedpolygon_part(poly->Parts()[0]);
            }   
            std::vector<GEOSGeometry*> geoms;
            for (const auto& pt: poly->Parts()) {
                geoms.push_back(make_complicatedpolygon_part(pt));
            }
            return GEOSGeom_createCollection_r(handle, 6, &geoms[0], geoms.size());
        }
              
};

std::shared_ptr<GeosGeometry> make_geos_geometry(std::shared_ptr<BaseGeometry> ele) { return std::make_shared<GeosGeometryImpl>(ele); }
std::shared_ptr<GeosGeometry> make_geos_geometry_cp_part(std::shared_ptr<ComplicatedPolygon> ele, size_t part) { return std::make_shared<GeosGeometryImpl>(ele, part); }
}
}
