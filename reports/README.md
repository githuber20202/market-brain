# Replay reports

`replay_<date>.json` records the simulated trades for one prior market session. The replay uses
historical one-minute SIP bars and the production structure, plan, trigger, and position-decision
engines. It never sends an order.

Each bar becomes four ordered price ticks. A rising or flat bar follows open, low, high, close; a
falling bar follows open, high, low, close. If a position is already open and one bar reaches both
its stop and its next profit target, the replay records the stop first. This is deliberately
conservative.

- `r` is profit or loss divided by the trade's initial risk. `1.0` means one unit of risk gained;
  `-1.0` means the initial risk was lost.
- `hit_rate` is the share of trades whose `r` is greater than zero.
- `expectancy_r` is the average `r` across all trades.
- `max_drawdown_r` is the largest peak-to-trough decline in the cumulative `r` sequence.

The simulation adds 10 basis points to a long entry, protects the fill immediately at the plan
stop, exits half at TP1 and the rest at TP2, and applies the production 30-minute time-stop rule.
Any remainder at the final available bar closes as a time stop. Replay plan scoring holds quality
at a neutral full-risk value so the report measures the deterministic price pipeline; it does not
claim that historical quality or catalysts were known.

