using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParserGeology
{
    public class WGS84toGSK2011Converter
    {
        private const double aW = 6378137; // Большая полуось для WGS-84
        private const double fW = 1.0 / 298.257223563; // Сжатие для WGS-84
        private static readonly double e2W = 2 * fW - Math.Pow(fW, 2); // Квадрат эксцентриситета

        // Параметры трансформации из WGS-84 в ГСК-2011
        private const double dx = 23.92;
        private const double dy = -141.27;
        private const double dz = -80.9;

        public (double Latitude, double Longitude, double Height) ConvertWGS84ToGSK2011(double Bd, double Ld, double H)
        {
            // Широта и долгота в радианах
            double B = Bd * Math.PI / 180;
            double L = Ld * Math.PI / 180;

            // Преобразование WGS-84 в XYZ
            double N = aW / Math.Sqrt(1 - e2W * Math.Sin(B) * Math.Sin(B));

            double X = (N + H) * Math.Cos(B) * Math.Cos(L);
            double Y = (N + H) * Math.Cos(B) * Math.Sin(L);
            double Z = (N * (1 - e2W) + H) * Math.Sin(B);

            // Применяем параметры трансформации
            X -= dx;
            Y -= dy;
            Z -= dz;

            // Преобразование обратно из XYZ в ГСК-2011
            double p = Math.Sqrt(X * X + Y * Y);
            double latitude = Math.Atan2(Z, p * (1 - fW)) * 180 / Math.PI;
            double longitude = Math.Atan2(Y, X) * 180 / Math.PI;

            return (latitude, longitude, H); // Высота остается неизменной
        }
    }
}
