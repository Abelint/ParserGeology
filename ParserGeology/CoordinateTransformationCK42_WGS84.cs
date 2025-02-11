using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ParserGeology
{
    public class CoordinateTransformationCK42_WGS84
    {
        private const double aP = 6378245; // Большая полуось для СК-42
        private const double fP = 1.0 / 298.3; // Сжатие для СК-42
        private double e2P = 2 * fP - Math.Pow(fP, 2); // Квадрат эксцентриситета
        private const double dx = 23.92; // Сдвиг по оси X
        private const double dy = -141.27; // Сдвиг по оси Y
        private const double dz = -80.9; // Сдвиг по оси Z

        public (double Latitude, double Longitude, double Height) ConvertSK42ToWGS84(double Bd, double Ld, double H)
        {
            // Преобразуем в XYZ
            double B = Bd * Math.PI / 180; // радианы
            double L = Ld * Math.PI / 180; // радианы

            double N = aP / Math.Sqrt(1 - e2P * Math.Sin(B) * Math.Sin(B));

            double X = (N + H) * Math.Cos(B) * Math.Cos(L);
            double Y = (N + H) * Math.Cos(B) * Math.Sin(L);
            double Z = (N * (1 - e2P) + H) * Math.Sin(B);

            // Применяем трансформацию
            X += dx; // Применяем смещения
            Y += dy;
            Z += dz;

            // Обратное преобразование от XYZ к широте, долготе, высоте
            double p = Math.Sqrt(X * X + Y * Y);
            double theta = Math.Atan2(Z * aP, p * (1 - fP));
            double sinTheta = Math.Sin(theta);
            double cosTheta = Math.Cos(theta);

            double latitude = Math.Atan2(Z + e2P * aP * Math.Pow(sinTheta, 3),
                                           p - e2P * aP * Math.Pow(cosTheta, 3)) * 180 / Math.PI;

            double longitude = Math.Atan2(Y, X) * 180 / Math.PI;

            // Возвращаем результат
            return (latitude, longitude, H); // высота остается неизменной
        }
    }
    
}
