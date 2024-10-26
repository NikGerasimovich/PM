-- Выбираем только значение где E-sport в промежутку с 12:00 14.03.2022 по 12:00 15.03.2022, где все события Prematch, ставки CashOut, возвраты, систнма и FreeBet не учитываются  
WITH cct1 AS (
    SELECT *
    FROM bets b
    LEFT JOIN events e ON b.event_id = e.event_id
    WHERE 
        e.sport = 'E-Sports' 
        AND b.accept_time >= '2022-03-14 12:00:00' 
        AND b.settlement_time <= '2022-03-15 12:00:00'
        AND b.event_stage = 'Prematch'
        AND b.bet_type != 'System' 
        AND b.is_free_bet = FALSE 
        AND b.item_result NOT LIKE 'Return%' 
        AND b.item_result != 'Cashout'
),
-- Сортировка по коэффиценту на событие не менее 1,5.
-- Для Express для каждого игрока формируем его Express где каждое событие этого Express не менее 1,5 
cct2 AS (
    SELECT *
    FROM cct1
    WHERE 
        (bet_type = 'Ordinar' AND accepted_bet_odd >= 1.5) 
        OR 
        (bet_type = 'Express' AND accepted_bet_odd >= 1.5 
            AND player_id IN (
                SELECT player_id
                FROM cct1
                WHERE bet_type = 'Express'
                GROUP BY player_id
                HAVING MIN(accepted_bet_odd) >= 1.5
            )
        )
)
-- Делаем сет player_id где мин сумма ставки 10 BYN
SELECT distinct player_id
FROM cct2
WHERE amount >= 10;
