namespace TwitterArchiveExtraktor
{
	using System;

	public class CoordinatesHelper
	{
		public static Tuple<double, double> GetAverageXY(dynamic coordinates)
		{
			double x = 0;
			double y = 0;

			foreach (dynamic coordinate in coordinates[0])
			{
				x += (double)coordinate[1];
				y += (double)coordinate[0];
			}

			return new Tuple<double, double>(x / coordinates[0].Count, y / coordinates[0].Count);
		}
	}
}