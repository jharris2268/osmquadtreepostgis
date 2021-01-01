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

#ifndef OSMQUADTREEPOSTGIS_VALIDATEGEOMS_HPP
#define OSMQUADTREEPOSTGIS_VALIDATEGEOMS_HPP

#include "oqt/geometry/elements/point.hpp"
#include "oqt/geometry/elements/linestring.hpp"
#include "oqt/geometry/elements/simplepolygon.hpp"
#include "oqt/geometry/elements/complicatedpolygon.hpp"

namespace oqt {
namespace geometry {

class GeosGeometry {
    public:
        virtual ~GeosGeometry() {}
        
        virtual void validate()=0;
        
        virtual std::string PointWkb()=0;
        virtual std::string Wkb()=0;
        virtual std::string BoundaryLineWkb()=0;
};

std::shared_ptr<GeosGeometry> make_geos_geometry(std::shared_ptr<BaseGeometry> ele);
std::shared_ptr<GeosGeometry> make_geos_geometry_cp_part(std::shared_ptr<ComplicatedPolygon> ele, size_t part);

}
}

#endif
