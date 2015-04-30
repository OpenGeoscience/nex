from netCDF4 import Dataset

def _copy_variable(target_file, source_file, variable):
    src_var = source_file.variables[variable]
    target_var = target_file.createVariable(variable, src_var.datatype, src_var.dimensions)
    target_var.setncatts({k: src_var.getncattr(k) for k in src_var.ncattrs()})
    target_var[:] = src_var[:]

def write(path, source, variable, data):
    output = Dataset(path, 'w')
    output.createDimension('lat', len(source.dimensions['lat']) if not source.dimensions['lat'].isunlimited() else None)
    output.createDimension('lon', len(source.dimensions['lon']) if not source.dimensions['lon'].isunlimited() else None)
    output.createDimension('time', len(source.dimensions['time']) if not source.dimensions['time'].isunlimited() else None)
    _copy_variable(output, source, 'lat')
    _copy_variable(output, source, 'lon')
    output.createVariable(variable, data.dtype, ('time', 'lat', 'lon'))
    output.variables[variable][:] = data
    output.close()
