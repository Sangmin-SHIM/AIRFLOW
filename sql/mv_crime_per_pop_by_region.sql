CREATE MATERIALIZED VIEW crime_per_pop_by_region AS
SELECT "Code.région",
       CAST(SUM(CASE WHEN classe = 'Homicides' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Homicides' THEN "POP" ELSE 1 END), 0) AS Homicides_per_pop,
       CAST(SUM(CASE WHEN classe = 'Coups et blessures volontaires' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Coups et blessures volontaires' THEN "POP" ELSE 1 END), 0) AS Coups_et_blessures_volontaires_per_pop,
       CAST(SUM(CASE WHEN classe = 'Coups et blessures volontaires intrafamiliaux' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Coups et blessures volontaires intrafamiliaux' THEN "POP" ELSE 1 END), 0) AS Coups_et_blessures_volontaires_intrafamiliaux_per_pop,
       CAST(SUM(CASE WHEN classe = 'Autres coups et blessures volontaires' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Autres coups et blessures volontaires' THEN "POP" ELSE 1 END), 0) AS Autres_coups_et_blessures_volontaires_per_pop,
       CAST(SUM(CASE WHEN classe = 'Violences sexuelles' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Violences sexuelles' THEN "POP" ELSE 1 END), 0) AS Violences_sexuelles_per_pop,
       CAST(SUM(CASE WHEN classe = 'Vols avec armes' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Vols avec armes' THEN "POP" ELSE 1 END), 0) AS Vols_avec_armes_per_pop,
       CAST(SUM(CASE WHEN classe = 'Vols violents sans arme' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Vols violents sans arme' THEN "POP" ELSE 1 END), 0) AS Vols_violents_sans_arme_per_pop,
       CAST(SUM(CASE WHEN classe = 'Vols sans violence contre des personnes' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Vols sans violence contre des personnes' THEN "POP" ELSE 1 END), 0) AS Vols_sans_violence_contre_des_personnes_per_pop,
       CAST(SUM(CASE WHEN classe = 'Cambriolages de logement' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Cambriolages de logement' THEN "POP" ELSE 1 END), 0) AS Cambriolages_de_logement_per_pop,
       CAST(SUM(CASE WHEN classe = 'Vols de véhicules' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Vols de véhicules' THEN "POP" ELSE 1 END), 0) AS Vols_de_véhicules_per_pop,
       CAST(SUM(CASE WHEN classe = 'Vols dans les véhicules' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Vols dans les véhicules' THEN "POP" ELSE 1 END), 0) AS Vols_dans_les_véhicules_per_pop,
       CAST(SUM(CASE WHEN classe = 'Vols d''accessoires sur véhicules' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Vols d''accessoires sur véhicules' THEN "POP" ELSE 1 END), 0) AS Vols_daccessoires_sur_véhicules_per_pop,
       CAST(SUM(CASE WHEN classe = 'Destructions et dégradations volontaires' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Destructions et dégradations volontaires' THEN "POP" ELSE 1 END), 0) AS Destructions_et_dégradations_volontaires_per_pop,
       CAST(SUM(CASE WHEN classe = 'Escroqueries' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Escroqueries' THEN "POP" ELSE 1 END), 0) AS Escroqueries_per_pop,
       CAST(SUM(CASE WHEN classe = 'Trafic de stupéfiants' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Trafic de stupéfiants' THEN "POP" ELSE 1 END), 0) AS Trafic_de_stupéfiants_per_pop,
       CAST(SUM(CASE WHEN classe = 'Usage de stupéfiants' THEN faits ELSE 0 END) AS FLOAT(2)) / NULLIF(SUM(CASE WHEN classe = 'Usage de stupéfiants' THEN "POP" ELSE 1 END), 0) AS Usage_de_stupéfiants_per_pop
FROM public.crime
WHERE "Code.région" IS NOT NULL
GROUP BY "Code.région"
ORDER BY "Code.région";