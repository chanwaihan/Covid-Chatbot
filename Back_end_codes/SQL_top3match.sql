SELECT TOP 3 FT_TBL.q_text, FT_TBL.q_id
FROM Question AS FT_TBL   
     INNER JOIN  
     FREETEXTTABLE(Question, q_text,  
                    'WHat is covid-19') AS KEY_TBL  
     ON FT_TBL.q_id = KEY_TBL.[KEY]  
WHERE KEY_TBL.RANK >= 5 AND q_type = 'CQ'
ORDER BY KEY_TBL.RANK DESC;
					
