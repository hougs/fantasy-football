
use super_football;

CREATE TABLE IF NOT EXISTS passing_sess AS
SELECT a.psr as player,
collect_all(named_struct(
  'pid', a.pid,
  'gid', b.gid,
  'yds', a.yds,
  'intcpt', cast(e.pid is not null as int),
  'td', cast(coalesce(c.pts, false) as int),
  'conv', cast(d.pid is not null as int))) as passes
FROM football.pass a
INNER JOIN football.core b ON a.pid = b.pid
LEFT JOIN football.scoring c on a.pid = c.pid
LEFT JOIN football.convs d ON a.pid = d.pid
LEFT JOIN football.ints e ON a.pid = e.pid
WHERE a.psr <> 'XX-0000'
GROUP BY a.psr;

CREATE TABLE IF NOT EXISTS passing_game_sess AS
SELECT a.psr as player, b.gid,
collect_all(named_struct(
  'pid', a.pid,
  'yds', a.yds,
  'intcpt', cast(e.pid is not null as int),
  'td', cast(coalesce(c.pts, false) as int),
  'conv', cast(d.pid is not null as int))) as passes
FROM football.pass a
INNER JOIN football.core b ON a.pid = b.pid
LEFT JOIN football.scoring c on a.pid = c.pid
LEFT JOIN football.convs d ON a.pid = d.pid
LEFT JOIN football.ints e ON a.pid = e.pid
WHERE a.psr <> 'XX-0000'
GROUP BY a.psr, b.gid;

CREATE TABLE IF NOT EXISTS rushing_sess AS
SELECT a.bc as player,
collect_all(named_struct(
  'pid', a.pid,
  'gid', b.gid,
  'yds', a.yds,
  'td', cast(coalesce(c.pts, false) as int),
  'conv', cast(d.pid is not null as int))) as rushes
FROM football.rush a
INNER JOIN football.core b ON a.pid = b.pid
LEFT JOIN football.scoring c on a.pid = c.pid
LEFT JOIN football.convs d ON a.pid = d.pid
LEFT JOIN football.fumbles e ON a.pid = e.pid
WHERE e.pid IS NULL AND a.bc <> 'XX-0000'
GROUP BY a.bc;

CREATE TABLE IF NOT EXISTS rushing_game_sess AS
SELECT a.bc as player, b.gid,
collect_all(named_struct(
  'pid', a.pid,
  'yds', a.yds,
  'td', cast(coalesce(c.pts, false) as int),
  'conv', cast(d.pid is not null as int))) as rushes
FROM football.rush a
INNER JOIN football.core b ON a.pid = b.pid
LEFT JOIN football.scoring c on a.pid = c.pid
LEFT JOIN football.convs d ON a.pid = d.pid
LEFT JOIN football.fumbles e ON a.pid = e.pid
WHERE e.pid IS NULL AND a.bc <> 'XX-0000'
GROUP BY a.bc, b.gid;

CREATE TABLE IF NOT EXISTS receiving_sess AS
SELECT a.trg as player,
collect_all(named_struct(
  'pid', a.pid,
  'gid', b.gid,
  'yds', a.yds,
  'td', cast(coalesce(c.pts, false) as int),
  'conv', cast(d.pid is not null as int))) as receptions
FROM football.pass a
INNER JOIN football.core b ON a.pid = b.pid
LEFT JOIN football.scoring c on a.pid = c.pid
LEFT JOIN football.convs d ON a.pid = d.pid
LEFT JOIN football.ints e ON a.pid = e.pid
WHERE e.pid IS NULL AND a.trg <> 'XX-0000'
GROUP BY a.trg;

CREATE TABLE IF NOT EXISTS receiving_game_sess AS
SELECT a.trg as player, b.gid,
collect_all(named_struct(
  'pid', a.pid,
  'yds', a.yds,
  'td', cast(coalesce(c.pts, false) as int),
  'conv', cast(d.pid is not null as int))) as receptions
FROM football.pass a
INNER JOIN football.core b ON a.pid = b.pid
LEFT JOIN football.scoring c on a.pid = c.pid
LEFT JOIN football.convs d ON a.pid = d.pid
LEFT JOIN football.ints e ON a.pid = e.pid
WHERE e.pid IS NULL AND a.trg <> 'XX-0000'
GROUP BY a.trg, b.gid;

CREATE TABLE IF NOT EXISTS fgxp_sess AS
SELECT a.fkicker as player,
collect_all(named_struct(
  'pid', a.pid,
  'gid', b.gid,
  'fgxp', a.fgxp,
  'dist', a.dist)) as fgxp
FROM football.fgxp a
INNER JOIN football.core b
ON a.pid = b.pid
WHERE a.good = 'Y' AND a.fkicker <> 'XX-0000'
GROUP BY a.fkicker;

CREATE TABLE IF NOT EXISTS fgxp_game_sess AS
SELECT a.fkicker as player,
b.gid,
collect_all(named_struct(
  'pid', a.pid,
  'fgxp', a.fgxp,
  'dist', a.dist)) as fgxp
FROM football.fgxp a
INNER JOIN football.core b
ON a.pid = b.pid
WHERE a.good = 'Y' AND a.fkicker <> 'XX-0000'
GROUP BY a.fkicker, b.gid;

CREATE TABLE IF NOT EXISTS scoring_sessions AS
SELECT coalesce(a.player, b.player) as player,
a.passes,
b.rushes, b.receptions, b.fgxp
FROM passing_sess a
FULL OUTER JOIN (
  SELECT coalesce(c.player, d.player) as player,
  c.rushes, d.receptions, d.fgxp
  FROM rushing_sess c
  FULL OUTER JOIN (
    SELECT coalesce(e.player, f.player) as player,
    e.receptions, f.fgxp
    FROM receiving_sess e
    FULL OUTER JOIN fgxp_sess f ON e.player = f.player
  ) d ON c.player = d.player
) b ON a.player = b.player;

CREATE TABLE IF NOT EXISTS scoring_game_sessions
STORED AS PARQUET AS
SELECT coalesce(a.player, b.player) as player,
coalesce(a.gid, b.gid) as gid,
a.passes,
b.rushes, b.receptions, b.fgxp
FROM passing_game_sess a
FULL OUTER JOIN (
  SELECT coalesce(c.player, d.player) as player,
  coalesce(c.gid, d.gid) as gid,
  c.rushes, d.receptions, d.fgxp
  FROM rushing_game_sess c
  FULL OUTER JOIN (
    SELECT coalesce(e.player, f.player) as player, coalesce(e.gid, f.gid) as gid,
    e.receptions, f.fgxp
    FROM receiving_game_sess e
    FULL OUTER JOIN fgxp_game_sess f ON e.player = f.player AND e.gid = f.gid
  ) d ON c.player = d.player AND c.gid = d.gid
) b ON a.player = b.player AND a.gid = b.gid;
