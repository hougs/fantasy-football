create table player_prediction as select player_game_points.player as player, games.seas as seas,
sum(coalesce(passing, 0) + coalesce(rushing, 0) + coalesce(receiving,0)) as totalPts,
stddev_pop(coalesce(passing, 0) + coalesce(rushing, 0) + coalesce(receiving,0)) as stddev, players.pos1 from player_game_points left join games on
playier_game_points.gid = games.gid left join players on players.player = player_game_points.player group by player_game_points.player, players.pos1, games.seas;

