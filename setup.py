from distutils.core import setup, Extension
from distutils.command.build_ext import build_ext
from distutils.sysconfig import customize_compiler

import shutil, os.path
import pybind11

#xmlinspector_path = os.path.abspath("../thirdparty/xmlinspector")

#if not os.path.exists(os.path.join(xmlinspector_path, "XmlInspector.hpp")):
#    raise Exception("specify path to xml-inspector headers")

postgresql_path = '/usr/include/postgresql'
if not os.path.exists(os.path.join(postgresql_path, "libpq-fe.h")):
    raise Exception("specify path to postgresql (libpq) headers")


#srcs=['src/'+f for f in ("oqt_python.cpp","block_python.cpp", "core_python.cpp","change_python.cpp", "geometry_python.cpp", "postgis_python.cpp")]




libs=['z','pq','stdc++fs', 'oqt', 'geos_c']

class my_build_ext(build_ext):
    def build_extensions(self):
        customize_compiler(self.compiler)
        try:
            self.compiler.compiler_so.remove("-Wstrict-prototypes")
        except (AttributeError, ValueError):
            pass
        build_ext.build_extensions(self)




ext_modules = []


srcs = ['src/processpostgis.cpp', 'src/postgiswriter.cpp', 'src/postgis_python.cpp', 'src/validategeoms.cpp']
modname = 'osmquadtreepostgis._osmquadtreepostgis'

ext_modules.append(

    Extension(
            modname,
            srcs,
            include_dirs=[
                '/usr/local/include',
                pybind11.get_include(),
                os.path.abspath('src/'),
                postgresql_path,
            ],
            libraries=libs,
            extra_compile_args=['-std=c++17',],
            
    )
)
    


setup(
    name='osmquadtreepostgis',
    packages=['osmquadtreepostgis'],
    version='0.0.1',
    long_description='',
    ext_modules=ext_modules,
    zip_safe=False,
    cmdclass = {'build_ext': my_build_ext},    
)
