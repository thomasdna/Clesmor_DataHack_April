# Gym business taxonomy (MVP)

This taxonomy is designed for an explainable 48-hour demo. It is **not** perfect classification.

## How it works
- We map POIs to roles by matching **keywords** against the official category fields we have:
  - `category_name`
  - `category_label`
- IDs can vary by release; keyword rules are the most robust MVP approach.

## Roles

### A) Direct competitors (examples)
Keywords include:
- gym, fitness, health club
- pilates, yoga
- boxing, martial arts
- crossfit, personal training
- recreation center, sports club

### B) Complementary ecosystem (examples)
Keywords include:
- cafes / coffee
- smoothie / juice
- health food retail, supermarkets, grocery, pharmacy
- physio / allied health, massage, chiropractor
- sports goods
- wellness services
- parks / trails

### C) Other commercial activity (proxy)
Keywords include:
- restaurants
- retail/shopping
- office/cowork
- school/university
- transit/station

### D) Optional exclusions
Empty by default. Add only when a category clearly pollutes results.

## Guardrails
- These are **proxies** for ecosystem/competition, not demand measurement.
- Mapping should be reviewed before any real-world decisioning.

