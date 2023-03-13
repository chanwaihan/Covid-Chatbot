DECLARE @sent_ids AS TABLE (sent_id NVARCHAR(100))

INSERT INTO @sent_ids
SELECT TOP 3 sent_id
FROM NUGGET_SENTENCE
WHERE sent_id IN
	(SELECT S.sent_id
			FROM ( ( (NUGGET AS N INNER JOIN QUESTION AS Q ON N.q_id = Q.q_id AND Q.q_text = 'what is the origin of COVID-19' AND Q.q_type = 'CQ')
					JOIN NUGGET_SENTENCE AS NS ON N.nggt_id = NS.nggt_id )
						JOIN SENTENCE AS S ON NS.sent_id = S.sent_id ) )
GROUP BY sent_id 
ORDER BY COUNT(nggt_id) ASC

SELECT SUBSTRING(cont_text, sent_start+1, sent_end) AS Answer
FROM (@sent_ids T1 JOIN SENTENCE S ON T1.sent_id = S.sent_id) JOIN CONTEXT C ON S.cont_id = C.cont_id
WHERE LEN(SUBSTRING(cont_text, sent_start+1, sent_end)) > 150