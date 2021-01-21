from tifffile import TiffFile


class RasterData:
    def __getIndexFromXY(self, x, y, nCols):
        index = y * nCols + x
        return index

    def __init__(self, file):
        with TiffFile(file)as tif:
            metadata = tif.geotiff_metadata
            scale = metadata["ModelPixelScale"]
            scaleX = scale[0]
            scaleY = scale[1]
            data = tif.asarray()
            shape = data.shape
            #x, lng
            nCols = shape[1]
            #y, lat
            nRows = shape[0]
            #tiepoint is i,j,k,x,y,z where i,j,k is the position of the xyz coordinates in raster space
            #note there can be multiple tiepoints, but this is unimportant for a flat 2d map (would only be relevant for a skewed plane in 3d space)
            #als k and z should be 0, just ignore
            tiepoint = metadata["ModelTiepoint"]
            #seems to be correct, madness
            #i is row (y)
            i = tiepoint[0]
            #j is column (x)
            j = tiepoint[1]
            #tiepoint[2] is k, skip
            x = tiepoint[3]
            y = tiepoint[4]
            #RS = raster space
            xllCornerRS = 0
            yllCornerRS = nRows
            xllOffsetRS = j - xllCornerRS
            yllOffsetRS = yllCornerRS - i
            xllOffset = xllOffsetRS * scaleX
            yllOffset = yllOffsetRS * scaleY
            xllCorner = x - xllOffset
            yllCorner = y - yllOffset

        #note nodata value not needed for application since strippping the values out 
            header = {
                "nCols": nCols,
                "nRows": nRows,
                "xllCorner": xllCorner,
                "yllCorner": yllCorner,
                "cellXSize": scaleX,
                "cellYSize": scaleY
            }
            #assume data[0][0] is nodata
            nodata = data[0][0]

            #let's add in 0 removal, create template of indices in header

            data_map = {}
            for yi in range(nRows):
                for xi in range(nCols):
                    value = data[yi][xi]
                    if value != nodata:
                        index = self.__getIndexFromXY(xi, yi, nCols)
                        data_map[index] = value


            self.header = header
            self.data = data_map


r = RasterData("1990_01_statewide_rf_mm.tif")
print(r.data)

        
    




