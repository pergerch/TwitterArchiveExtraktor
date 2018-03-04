namespace TwitterArchiveExtraktor.Model
{
	public class Tweet
	{
		public string Comment { get; set; }

		public string CreatedAt { get; set; }

		public string Hashtag { get; set; }

		public string Id { get; set; }

		public bool Retweet { get; set; }

		public string Text { get; set; }

		public string Username { get; set; }

		public double? X { get; set; }

		public double? Y { get; set; }
	}
}