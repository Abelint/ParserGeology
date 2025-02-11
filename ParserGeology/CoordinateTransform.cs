

using GeoAPI.CoordinateSystems.Transformations;
using GeoAPI.CoordinateSystems;

using ProjNet.Converters.WellKnownText;
using ProjNet.CoordinateSystems.Transformations;



using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ProjNet.CoordinateSystems;

namespace ParserGeology
{
    public class CoordinateTransform
    {
        public (double Latitude, double Longitude, double Height) ConvertGSK2011ToWGS84(double x, double y, double height)
        {
            var csf = new CoordinateSystemFactory();
            // Определяем системы координат
            var gsk2011 = csf.CreateFromWkt(
                "PROJCS[\"GSK-2011\", GEOGCS[\"GSK-2011\", DATUM[\"GSK-2011_Datum\", SPHEROID[\"GRS 1980\", 6378137, 298.257222101]], PRIMEM[\"Greenwich\", 0], UNIT[\"degree\", 0.0174532925199433]], PROJECTION[\"Transverse_Mercator\"], PARAMETER[\"latitude_of_origin\", 0], PARAMETER[\"central_meridian\", 45], PARAMETER[\"scale_factor\", 1], PARAMETER[\"false_easting\", 2500000], PARAMETER[\"false_northing\", 1500000], UNIT[\"m\", 1]]"
            );

            var wgs84 = csf.CreateFromWkt(
                "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]]"
            );

            // Создаем трансформер
            var transformation = new CoordinateTransformationFactory().CreateFromCoordinateSystems(gsk2011, wgs84);

            // Преобразуем координаты
            double[] gsk2011Coords = { x, y, height };
            double[] wgs84Coords = transformation.MathTransform.Transform(gsk2011Coords);

            // Возвращаем результат
            return (wgs84Coords[1], wgs84Coords[0], wgs84Coords[2]); // широта, долгота, высота
        }


        public double[] FunToWGS(string CoordinateSystem, double[] inputCoordinate)
        {

            //Coordinate coord = new Coordinate(this.Lon, this.Lat,0);
            //string wkt_WGS84N40 = "PROJCS[\"WGS_1984_UTM_Zone_40N\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",57.0],PARAMETER[\"Scale_Factor\",0.9996],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
            string wkt_WGS84N40 = CoordinateSystem;
            IProjectedCoordinateSystem gcs_WGS84_IProj = null;
            IGeographicCoordinateSystem gcs_WGS84_IGeo = null;
            if (wkt_WGS84N40.Split('[')[0] == "PROJCS")
            {
                gcs_WGS84_IProj = (IProjectedCoordinateSystem?)CoordinateSystemWktReader.Parse(wkt_WGS84N40);
            }
            else if (wkt_WGS84N40.Split('[')[0] == "GEOGCS")
            {
                gcs_WGS84_IGeo = CoordinateSystemWktReader.Parse(wkt_WGS84N40) as IGeographicCoordinateSystem;
            }
            else return null;


            // IProjectedCoordinateSystem gcs_WGS84 = CoordinateSystemWktReader.Parse(wkt_WGS84N40) as IProjectedCoordinateSystem;



            //Destination  
            string wkt_WgsGeo = "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]]";
            //string wkt_WgsGeo = subjectsRF[0].WKT;


            IGeographicCoordinateSystem geo = CoordinateSystemWktReader.Parse(wkt_WgsGeo) as IGeographicCoordinateSystem;
            CoordinateTransformationFactory ctfac = new CoordinateTransformationFactory();
            double[] output = null;
            try
            {
                //ICoordinateTransformation trans = ctfac.CreateFromCoordinateSystems(gcs_WGS84, geo);
                if (gcs_WGS84_IProj != null)
                {
                    ICoordinateTransformation trans = ctfac.CreateFromCoordinateSystems(gcs_WGS84_IProj, geo);
                    output = trans.MathTransform.Transform(inputCoordinate);
                }
                else if (gcs_WGS84_IGeo != null)
                {
                    ICoordinateTransformation trans = ctfac.CreateFromCoordinateSystems(gcs_WGS84_IGeo, geo);
                    output = trans.MathTransform.Transform(inputCoordinate);
                }
                //Console.WriteLine("Original Coordinates:");  
                // double[] fromPoint = inputCoordinate; // U2U Consult Head Office, in degrees  
                // output = trans.MathTransform.Transform(inputCoordinate);
            }
            catch (Exception e)
            {
                
            }
            return output;
        }

        public double[] FunFromWGS(string CoordinateSystem, double[] inputCoordinate)
        {

            //Coordinate coord = new Coordinate(this.Lon, this.Lat,0);
            //string wkt_WGS84N40 = "PROJCS[\"WGS_1984_UTM_Zone_40N\",GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]],PROJECTION[\"Transverse_Mercator\"],PARAMETER[\"False_Easting\",500000.0],PARAMETER[\"False_Northing\",0.0],PARAMETER[\"Central_Meridian\",57.0],PARAMETER[\"Scale_Factor\",0.9996],PARAMETER[\"Latitude_Of_Origin\",0.0],UNIT[\"Meter\",1.0]]";
            string wkt_WGS84N40 = "GEOGCS[\"GCS_WGS_1984\",DATUM[\"D_WGS_1984\",SPHEROID[\"WGS_1984\",6378137.0,298.257223563]],PRIMEM[\"Greenwich\",0.0],UNIT[\"Degree\",0.0174532925199433]]";
            //IProjectedCoordinateSystem gcs_WGS84 = CoordinateSystemWktReader.Parse(wkt_WGS84N40) as IProjectedCoordinateSystem;
            IGeographicCoordinateSystem gcs_WGS84 = CoordinateSystemWktReader.Parse(wkt_WGS84N40) as IGeographicCoordinateSystem;
            //Destination  
            string wkt_WgsGeo = CoordinateSystem;
            //string wkt_WgsGeo = subjectsRF[0].WKT;


            IProjectedCoordinateSystem geo = CoordinateSystemWktReader.Parse(wkt_WgsGeo) as IProjectedCoordinateSystem;
            CoordinateTransformationFactory ctfac = new CoordinateTransformationFactory();
            double[] output;
            try
            {
                ICoordinateTransformation trans = ctfac.CreateFromCoordinateSystems(gcs_WGS84, geo);
                //Console.WriteLine("Original Coordinates:");  
                // double[] fromPoint = inputCoordinate; // U2U Consult Head Office, in degrees  
                output = trans.MathTransform.Transform(inputCoordinate);
            }
            catch (Exception e)
            {
                //CoordinateTrouble coordinateTrouble = new CoordinateTrouble();
                //if (coordinateTrouble.ShowDialog() == true)
                //{
                output = inputCoordinate;
                //}
                //else return null;


            }
            return output;
        }
    }
}
