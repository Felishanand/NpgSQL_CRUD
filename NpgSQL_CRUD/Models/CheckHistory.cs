using System;

namespace NpgSQL_CRUD
{
    public class CheckHistory
    {
        public Guid Id { get; set; }

        public string AssetName { get; set; }

        public string ClusterName { get; set; }

        public string AssetType { get; set; }

        public string AssetClass { get; set; }

        public DateTime StartDate { get; set; }

        public DateTime EndDate { get; set; }

        public bool CollectionTimeResult { get; set; }
    }
}