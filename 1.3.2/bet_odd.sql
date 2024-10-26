WITH bet_odds AS (
    SELECT 
        bet_id,
        EXP(SUM(LOG(odd))) AS bet_odd  -- Вычисляем общее значение bet_odd
    FROM 
        bets
    GROUP BY 
        bet_id
)

SELECT 
    b.bet_id,
    b.event_id,
    b.odd,
    bo.bet_odd
FROM 
    bets b
JOIN 
    bet_odds bo ON b.bet_id = bo.bet_id;  -- Объединяем с подзапросом
