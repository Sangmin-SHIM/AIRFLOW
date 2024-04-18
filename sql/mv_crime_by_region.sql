CREATE MATERIALIZED VIEW crime_by_region AS
SELECT "Code.région",
       SUM(CASE WHEN classe = 'Homicides' THEN faits ELSE 0 END) AS Homicides,
       SUM(CASE WHEN classe = 'Coups et blessures volontaires' THEN faits ELSE 0 END) AS Coups_et_blessures_volontaires,
       SUM(CASE WHEN classe = 'Coups et blessures volontaires intrafamiliaux' THEN faits ELSE 0 END) AS Coups_et_blessures_volontaires_intrafamiliaux,
       SUM(CASE WHEN classe = 'Autres coups et blessures volontaires' THEN faits ELSE 0 END) AS Autres_coups_et_blessures_volontaires,
       SUM(CASE WHEN classe = 'Violences sexuelles' THEN faits ELSE 0 END) AS Violences_sexuelles,
       SUM(CASE WHEN classe = 'Vols avec armes' THEN faits ELSE 0 END) AS Vols_avec_armes,
       SUM(CASE WHEN classe = 'Vols violents sans arme' THEN faits ELSE 0 END) AS Vols_violents_sans_arme,
       SUM(CASE WHEN classe = 'Vols sans violence contre des personnes' THEN faits ELSE 0 END) AS Vols_sans_violence_contre_des_personnes,
       SUM(CASE WHEN classe = 'Cambriolages de logement' THEN faits ELSE 0 END) AS Cambriolages_de_logement,
       SUM(CASE WHEN classe = 'Vols de véhicules' THEN faits ELSE 0 END) AS Vols_de_véhicules,
       SUM(CASE WHEN classe = 'Vols dans les véhicules' THEN faits ELSE 0 END) AS Vols_dans_les_véhicules,
       SUM(CASE WHEN classe = 'Vols d''accessoires sur véhicules' THEN faits ELSE 0 END) AS Vols_daccessoires_sur_véhicules,
       SUM(CASE WHEN classe = 'Destructions et dégradations volontaires' THEN faits ELSE 0 END) AS Destructions_et_dégradations_volontaires,
       SUM(CASE WHEN classe = 'Escroqueries' THEN faits ELSE 0 END) AS Escroqueries,
       SUM(CASE WHEN classe = 'Trafic de stupéfiants' THEN faits ELSE 0 END) AS Trafic_de_stupéfiants,
       SUM(CASE WHEN classe = 'Usage de stupéfiants' THEN faits ELSE 0 END) AS Usage_de_stupéfiants
FROM public.crime
WHERE "Code.région" IS NOT NULL
GROUP BY "Code.région"
ORDER BY "Code.région";