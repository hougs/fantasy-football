CREATE TABLE player_game_points
STORED AS PARQUET
AS SELECT a.player, a.gid,
b.*, c.*, d.*
FROM scoring_game_sessions a
LATERAL VIEW
within_table(array(
  "select sum(yds) yds, sum(intcpt) intcpt, sum(td) td, sum(conv) conv from t1",
  "select (yds/25) - 2*intcpt + 2*conv + 4*td as passing from last"), passes) b
LATERAL VIEW
within_table(array(
  "select sum(yds) yds, sum(td) td, sum(conv) conv from t1",
  "select (yds/10) + 6*td + 2*conv as rushing from last"), rushes) c
LATERAL VIEW
within_table(array(
  "select sum(yds) yds, sum(td) td, sum(conv) conv from t1",
  "select (yds/10) + 6*td + 2*conv as receiving from last"), receptions) d
