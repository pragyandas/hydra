# Scale suggestion

The following are rough estimates for scaling Hydra to different cluster sizes.

### Please note:
- These are not exact numbers and shouldn't be considered prescriptive
- The intention is rhetorical and to give you a good starting point to start thinking about the scale of the cluster you need to run.

## Small cluster

Regions: 1 - 3
Per Region:
- Nodes: 3 - 4
- Buckets: 15 - 20
- Actors: ~2000

Total:
- Nodes:   6-12
- Buckets: 30-60
- Actors:  4,000-6,000

## Medium cluster

Regions: 4-6
Per Region:
- Nodes:   10-15
- Buckets: 100
- Actors:  ~50,000

Total:
- Nodes:   40-90
- Buckets: 400-600
- Actors:  200,000-300,000


## Large cluster

Regions: 8-10 (multiple / continent)
Per Region:
- Nodes:   20-30
- Buckets: 200-300
- Actors:  ~200,000

Total:
- Nodes:   160-300
- Buckets: 1,600-3,000
- Actors:  ~2M

## Massive cluster

Regions: 15-20 (global coverage)
Per Region:
- Nodes:   50-100
- Buckets: 500-1000
- Actors:  ~1M

Total:
- Nodes:   750-2000
- Buckets: 7,500-20,000
- Actors:  15M-20M