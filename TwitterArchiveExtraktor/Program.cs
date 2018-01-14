namespace TwitterArchiveExtraktor
{
	using System;
	using System.Collections.Concurrent;
	using System.Collections.Generic;
	using System.IO;
	using System.Text;
	using System.Threading.Tasks;
	using CsvHelper;
	using ICSharpCode.SharpZipLib.BZip2;
	using Newtonsoft.Json.Linq;
	using TwitterArchiveExtraktor.Model;

	public class Program
	{
		private static ConcurrentDictionary<string, int> Hashtags = new ConcurrentDictionary<string, int>();

		private static object locker = new Object();

		public static string BasePath => @"C:\Users\Christoph\Downloads\archiveteam-twitter-stream-2015-05\";

		public static string OutputPath => @"C:\Users\Christoph\Downloads\archiveteam-twitter-stream-2015-05\";

		public static void IterateOverDays()
		{
			// 4. ExportTweetsWithHashtags
			//TweetByHashtagExporter exporter =
			//	new TweetByHashtagExporter(BasePath, new List<string> { "sheet1.csv", "sheet2.csv", "sheet3.csv" });

			foreach (string day in Directory.GetDirectories(BasePath))
			{
				string dayName = new DirectoryInfo(day).Name;
				foreach (string hour in Directory.GetDirectories(day))
				{
					string hourName = new DirectoryInfo(hour).Name;

					Parallel.ForEach(Directory.GetFiles(hour, "*.bz2"), (minute) =>
					//foreach (string minute in Directory.GetFiles(hour, "*.bz2"))
					{
						string minuteName = new DirectoryInfo(minute).Name.Substring(0, 2);

						//Console.WriteLine($"==> day: {day}, hour: {hour}, minute: {minute}");
						using (FileStream zippedFile = File.OpenRead(minute))
						{
							using (MemoryStream memoryStream = new MemoryStream())
							{
								BZip2.Decompress(zippedFile, memoryStream, true);

								string minuteUnzipped = Encoding.UTF8.GetString(memoryStream.ToArray());

								// 1.
								//ExtractTweets(minuteUnzipped, dayName, hourName);

								// 2.
								//CountTweets(minuteUnzipped, dayName, hourName, minuteName);

								// 3.
								//CountHashtags(minuteUnzipped, dayName, hourName, minuteName);

								// 4.
								//exporter.ExportTweetsWithHashtags(minuteUnzipped);

								// 5.
								ExtractGeoTweets(minuteUnzipped, dayName, hourName);
							}
						}
					});

					Console.WriteLine($"{dayName}.May - {hourName}:00 finished.");
				}
			}

			// 3. CountHashtags
			//WriteHashtags();
		}

		public static void Main(string[] args)
		{
			IterateOverDays();
		}

		private static void CountHashtags(string tweets, string day, string hour, string minute)
		{
			string[] lines = tweets.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

			foreach (string line in lines)
			{
				if (line.StartsWith("{\"created_at\":"))
				{
					dynamic tweet = JObject.Parse(line);

					if (tweet.lang == "en")
					{
						if (tweet.entities.hashtags.Count > 0)
						{
							foreach (dynamic hashtag in tweet.entities.hashtags)
							{
								string tag = hashtag.text.ToString().ToLower();
								if (Program.Hashtags.ContainsKey(tag))
								{
									Program.Hashtags[tag] = ++Program.Hashtags[tag];
								}
								else
								{
									Program.Hashtags[tag] = 1;
								}
							}
						}
					}
				}
			}
		}

		private static void CountTweets(string tweets, string day, string hour, string minute)
		{
			string[] lines = tweets.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

			List<string> outputLines = new List<string>();
			int totalTweets = 0, englishTweets = 0, englishTweetsWithHashtags = 0, englishTweetsWithUserMentions = 0;

			foreach (string line in lines)
			{
				if (line.StartsWith("{\"created_at\":"))
				{
					totalTweets++;
					dynamic tweet = JObject.Parse(line);

					if (tweet.lang == "en")
					{
						englishTweets++;

						if (tweet.entities.hashtags.Count > 0)
						{
							englishTweetsWithHashtags++;
						}

						if (tweet.entities.user_mentions.Count > 0)
						{
							englishTweetsWithUserMentions++;
						}
					}
				}
			}

			outputLines.Add(
				$"{day},{hour},{minute},{totalTweets},{englishTweets},{englishTweetsWithHashtags},{englishTweetsWithUserMentions}");

			string filename = OutputPath + "count.txt";

			lock (Program.locker)
			{
				File.AppendAllLines(filename, outputLines);
			}
		}

		private static void ExtractTweets(string tweets, string day, string hour)
		{
			string[] lines = tweets.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

			List<string> outputLines = new List<string>();

			foreach (string line in lines)
			{
				//Console.WriteLine(line.Substring(0, 120));

				if (line.StartsWith("{\"created_at\":"))
				{
					dynamic tweet = JObject.Parse(line);

					if (tweet.lang == "en")
					{
						//Console.WriteLine($"tweet id: {tweet.id}, created at: {tweet.created_at}, text: {tweet.text}");

						outputLines.Add(tweet.text.ToString());
					}
				}
			}

			string filename = OutputPath + $"text-{day}-{hour}.txt";

			File.AppendAllLines(filename, outputLines);
		}

		private static void ExtractGeoTweets(string tweets, string day, string hour)
		{
			string[] lines = tweets.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

			List<Tweet> output = new List<Tweet>();

			foreach (string line in lines)
			{
				//Console.WriteLine(line.Substring(0, 120));

				if (line.StartsWith("{\"created_at\":"))
				{
					dynamic tweet = JObject.Parse(line);

					if (tweet.lang == "en")
					{
						if (tweet.coordinates != null)
						{
							output.Add(new Tweet
							{
								Id = tweet.id,
								//Hashtag = String.Join("|", tweet.entities?.hashtags?.  text.ToString(),
								CreatedAt = tweet.created_at,
								Username = tweet.user.screen_name,
								Text = tweet.text.ToString(),
								Y = tweet.coordinates.coordinates[0],
								X = tweet.coordinates.coordinates[1]
							});
						}
					}
				}
			}

			string filename = OutputPath + $"geo.csv";

			lock (locker)
			{
				if (output.Count > 0)
				{
					using (TextWriter writer = File.AppendText(filename))
					{
						CsvWriter csv = new CsvWriter(writer);
						csv.Configuration.SanitizeForInjection = false;
						csv.Configuration.HasHeaderRecord = false;
						csv.Configuration.Delimiter = ";";
						csv.WriteRecords(output);
					}
				}
			}
		}

		private static void WriteHashtags()
		{
			List<string> outputLines = new List<string>();
			foreach (KeyValuePair<string, int> hashtag in Program.Hashtags)
			{
				outputLines.Add($"{hashtag.Key},{hashtag.Value}");
			}

			string filename = OutputPath + "countHashtags.txt";

			File.AppendAllLines(filename, outputLines);
		}
	}
}