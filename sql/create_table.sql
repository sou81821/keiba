create table horse(horse_id varchar(20), name varchar(30), tihou_horse_flg int, foreign_horse_flg int, sex varchar(10), hair varchar(30), father varchar(30), mother varchar(30), m_father varchar(30), birthday varchar(30), tyokyoshi text, owner text, price text);

create table race(race_id varchar(20), race_name text, race_year int, race_place text, race_time int, race_day int, race_order int, race_type varchar(20), race_direction varchar(20), race_distance int, race_whether varchar(20), race_baba varchar(20), lap text, pace text);

create table result(horse_id varchar(20), race_id varchar(20), goal varchar(20), waku_ban int, uma_ban int, horse_name text, horse_sex varchar(20), horse_age int, weight int, jockey text, race_time int, margin text, passing_first varchar(20), passing_second varchar(20), passing_third varchar(20), passing_forth varchar(20), rise real, win_odds real, popularity int, horse_weight int, horse_weight_change int, tyokyoshi text, owner text, prize real);

create table odds(race_id varchar(20), type text, kaime text, odds int, popularity int);