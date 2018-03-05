namespace TwitterArchiveExtraktor
{
	using System;
	using System.Collections.Generic;
	using System.IO;
	using System.Linq;
	using CsvHelper;
	using Newtonsoft.Json.Linq;
	using TwitterArchiveExtraktor.Model;

	public class TweetByHashtagExporter
	{
		private string BasePath;

		private Dictionary<string, List<string>> hashtagCategories;

		private object locker = new object();

		public TweetByHashtagExporter(string basePath, List<string> csvFilenames)
		{
			this.BasePath = basePath;
			this.hashtagCategories = new Dictionary<string, List<string>>();

			foreach (string csvFilename in csvFilenames)
			{
				string[] hashtags = File.ReadAllLines(basePath + "\\" + csvFilename);
				this.hashtagCategories.Add(csvFilename, hashtags.Select(x => x.ToLower()).ToList());
			}
		}

		public void ExportTweetsWithHashtags(string tweets)
		{
			string[] lines = tweets.Split(new[] { Environment.NewLine }, StringSplitOptions.None);

			Dictionary<string, List<Tweet>> tweetExport = new Dictionary<string, List<Tweet>>();
			foreach (KeyValuePair<string, List<string>> hashtagCategory in this.hashtagCategories)
			{
				tweetExport.Add(hashtagCategory.Key, new List<Tweet>());
			}

			foreach (string line in lines)
			{
				if (line.StartsWith("{\"created_at\":"))
				{
					dynamic tweet = JObject.Parse(line);

					if (tweet.lang != "en" || tweet.entities.hashtags.Count == 0)
					{
						continue;
					}

					foreach (KeyValuePair<string, List<string>> hashtagCategory in this.hashtagCategories)
					{
						foreach (dynamic hashtag in tweet.entities.hashtags)
						{
							string tag = hashtag.text.ToString().ToLower();
							if (hashtagCategory.Value.Contains(tag))
							{
								string comment = null;
								bool retweet = false;
								if (tweet.retweeted_status != null)
								{
									comment = tweet.retweeted_status.text.ToString();
									retweet = true;
								}

								tweetExport[hashtagCategory.Key].Add(new Tweet
								{
									Id = tweet.id,
									Hashtag = tag,
									CreatedAt = tweet.created_at,
									Username = tweet.user.screen_name,
									Text = tweet.text.ToString(),
									Comment = comment,
									Retweet = retweet
								});

								//tweetExport[hashtagCategory.Key].Add(new Tweet
								//{
								//	Id = tweet.id,
								//	Hashtag = tag,
								//	CreatedAt = tweet.created_at,
								//	Username = tweet.user.screen_name,
								//	Text = tweet.text.ToString()
								//});
							}
						}
					}
				}
			}

			lock (this.locker)
			{
				foreach (KeyValuePair<string, List<Tweet>> pair in tweetExport)
				{
					if (pair.Value.Any())
					{
						using (TextWriter writer = File.AppendText(this.BasePath + "\\ex_" + pair.Key))
						{
							CsvWriter csv = new CsvWriter(writer);
							csv.Configuration.SanitizeForInjection = false;
							csv.Configuration.HasHeaderRecord = false;
							csv.Configuration.Delimiter = ";";
							csv.WriteRecords(pair.Value);
						}
					}
				}
			}
		}
	}
}