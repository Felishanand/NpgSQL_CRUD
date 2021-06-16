using System;

namespace NpgSQL_CRUD.Models
{
    public class DayWiseAvailability
    {
        public Guid Id { get; set; }

        public string AssetName { get; set; }

        public string AssetClass { get; set; }

        public string AssetType { get; set; }

        public string ClusterName { get; set; }

        public DateTime CollectionDate { get; set; }

        public string AvailabilityPercentage { get; set; }
    }
}