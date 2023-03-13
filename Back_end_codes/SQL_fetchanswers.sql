SELECT DISTINCT S.sent_id, SUBSTRING(C.cont_text, S.sent_start+1, S.sent_end) AS Answer
FROM ( ( (NUGGET AS N INNER JOIN QUESTION AS Q ON N.q_id = Q.q_id AND Q.q_id = 'EQ023' AND Q.q_type = 'EQ')
		JOIN NUGGET_SENTENCE AS NS ON N.nggt_id = NS.nggt_id )
			JOIN SENTENCE AS S ON NS.sent_id = S.sent_id )  
				JOIN CONTEXT AS C ON S.cont_id = C.cont_id
WHERE LEN(SUBSTRING(C.cont_text, S.sent_start+1, S.sent_end)) > 80
ORDER BY S.sent_id ASC

