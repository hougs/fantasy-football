#!/bin/bash

hive -e "use super_football_new;select players.player, player_pos_predict.seas, player_pos_predict.totalPts, player_pos_predict.stddev from player_pos_predict join players on players.player = player_pos_predict.player where player_pos_predict.pos1='WR'" > ~/temp/WR.txt

hive -e "use super_football_new;select players.player, player_pos_predict.seas, player_pos_predict.totalPts, player_pos_predict.stddev from player_pos_predict join players on players.player = player_pos_predict.player where player_pos_predict.pos1='RB'" > ~/temp/RB.txt

hive -e "use super_football_new;select players.player, player_pos_predict.seas, player_pos_predict.totalPts, player_pos_predict.stddev from player_pos_predict join players on players.player = player_pos_predict.player where player_pos_predict.pos1='QB'" > ~/temp/QB.txt

hive -e "use super_football_new;select players.player, player_pos_predict.seas, player_pos_predict.totalPts, player_pos_predict.stddev from player_pos_predict join players on players.player = player_pos_predict.player where player_pos_predict.pos1='TE" > ~/temp/TE.txt

