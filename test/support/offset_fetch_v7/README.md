# Official Kafka OffsetFetch codec vectors

`manifest.json` is copied unchanged from investigation `java-probe/fixture-generation/vectors/manifest.json`, generated offline by Apache Kafka Java client 4.0.2's official message classes. The manifest embeds exact frames, decoded fields, generator source SHA256, and Kafka client JAR SHA256. These are wire fixtures, not evidence of broker semantics.

The final response partition deliberately contains signed minimum offset/epoch, which this client rejects as semantically invalid (the existing v5 rule also rejects it). Tests first assert rejection, then replace only these two boundary fields with -1 to exercise all other exact response fields and tags. Request comparisons normalize only the dynamic correlation ID and fixed client ID, leaving official body and flexible header tag bytes intact.
