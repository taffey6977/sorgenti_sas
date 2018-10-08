/************************************************/
/* 												*/
/* selezione coorte per analisi mortalita FAR	*/
/* 												*/
/************************************************/
/* orpss@regione.veneto.it						*/
/************************************************/
/* LEFT_OUTER 																		*/
%MACRO LEFT_OUTER(db1,db2,by,db3);
proc sort data=&db1;by &by;run;
proc sort data=&db2;by &by;run;
data &db3;
merge &db1 (in=a)  &db2 (in=b);
if a; 
by &by;
run;
%MEND;                                            										
/* INNER 																			*/
%MACRO INNER(db1,db2,by,db3);
proc sort data=&db1;by &by;run;
proc sort data=&db2;by &by;run;
data &db3;
merge &db1 (in=a)  &db2 (in=b);
if a and b ; 
by &by;
run;
%MEND;
/* seleziono i dati per gli anni di invio 2015-17*/
proc sql;
	create table M as
	select *
	from DWHBT.BTDETT_FAR_ADT
	where (ANNO_TRASM = '2015' OR ANNO_TRASM = '2016' OR ANNO_TRASM = '2017');
quit;
/*
DATA_INGRESSO_UDO indica la data di ingresso nell'UDO per episodio di presa in carico 
(quindi i soggetti che entrano tra il 2015 e il 2017) 
DATA_INGRESSO indica la data di primo ingresso _ever_ in FAR, deve essere >1/1/2015
*/
data m1;
	set m;
	if 2015 <= YEAR(DATA_INGRESSO_UDO) <= 2017 and YEAR(DATA_INGRESSO) >= 2015 then
		output; /*198813*/
run;
/*
tengo solo le UDO di primo e secondo livello (compresi i religiosi)
quindi COD_TIPO_UDO 01 1 02 2 07 7 08 8
*/
data m1; set m1; 
where COD_TIPO_UDO in ('01' '1' '02' '2' '07' '7' '08' '8');run; 
/*
definisco variabile tipo_udo=1 primo livello; 2 secondo livello
*/
data m1; set m1; 
if cod_tipo_udo in ('01' '1' '07' '7') then tipo_udo=1;
if cod_tipo_udo in ('02' '2' '08' '8') then tipo_udo=2;
run; 
/*ordino per data di ingresso udo*/
proc sort data=m1; by codice_soggetto data_ingresso_udo; run; 
/*counter prima data ingresso udo*/
data m1;
set m1;
by codice_soggetto;
if first.codice_soggetto then count_date_in=1 ; else count_date_in+1;
run;
/*
mi tengo una tabella separata solo con codice soggetto e data di primo ingresso udo
*/
data primo_ingresso;
	set m1;
	where count_date_in=1;
run;

data primo_ingresso;
	set primo_ingresso;
	keep codice_soggetto data_ingresso_udo;
run;

proc sort nodupkey data= primo_ingresso;
	by codice_soggetto;
run; /*per scrupolo*/

data primo_ingresso;
	set primo_ingresso;
	rename data_ingresso_udo = data_primo_ingresso_udo;
run;
/*
se manca la data di uscita o se la data di uscita è superiore al 31/12/2017 la converto al 31/12/2017
*/
data m1; 
set m1;
if year(data_dimissione)=2018 	then data_dimissione1=mdy(12,31,2017);
if data_dimissione eq . 		then data_dimissione1=mdy(12,31,2017);
if data_dimissione ne . and year (data_dimissione ) ne 2018 then data_dimissione1=data_dimissione; 
format data_dimissione1 $date9.; 
run; 
/*ordino per ultima data di uscita udo*/
proc sort data=m1;
	by codice_soggetto descending data_dimissione1;
run;
/*counter ultima data uscita udo*/
data m1;
	set m1;
	by codice_soggetto;

	if first.codice_soggetto then
		count_date_out=1;
	else count_date_out+1;
run;
/*
mi tengo una tabella separata solo con codice soggetto e data di ultima uscita udo
*/
data ultima_uscita;
	set m1;
	where count_date_out=1;
run;

data ultima_uscita;
	set ultima_uscita;
	keep codice_soggetto data_dimissione1;
run;

proc sort nodupkey data= ultima_uscita;
	by codice_soggetto;
run; /*per scrupolo*/

data ultima_uscita;
	set ultima_uscita;
	rename data_dimissione1 = data_ultima_dimissione;
run;
/*
faccio un link tra primo_ingresso e ultima_uscita per calcolare i giorni totali
*/
%inner(primo_ingresso, ultima_uscita, codice_soggetto, out);

data out;
	set out;
	giorni_permanenza_lordi = data_ultima_dimissione - data_primo_ingresso_udo;
run;

/* 
per calcolare le assenze totali mi creo un db solo con le assenze 
tengo solo i record che hanno compilate entrambe le date 
*/
data assenze;
	set m1 (keep= codice_soggetto 
		data_fi_assenza_temp
		data_in_assenza_temp);
	where data_in_assenza_temp ne . and data_fi_assenza_temp ne .;
run;
/*
tolgo i soggetti duplicati per chiave codice_soggetto data_in_assenza_temp data_fi_assenza_temp
*/
proc sort nodupkey data=assenze; by codice_soggetto data_in_assenza_temp data_fi_assenza_temp; run; 
/*
conteggio durata assenza per soggetto 
*/
data assenze;
	set assenze;
	durata_assenza = data_fi_assenza_temp - data_in_assenza_temp;
run;

data assenze;
	set assenze; if durata_assenza <0 then durata_assenza = 0;
run;

proc summary data=assenze nway;
	class codice_soggetto;
	var durata_assenza;
	output out=conteggio_assenze (drop = _type_ _freq_) sum(durata_assenza)=somma_assenze;
run;

/*
attacco il db conteggio assenze al db del conteggio giornate per calcolare i giorni netti
*/
%left_outer(out, conteggio_assenze, codice_soggetto, out1);
data out1;
	set out1;

	if somma_assenze ne . then
		giorni_permanenza_netti = giorni_permanenza_lordi - somma_assenze;
	else giorni_permanenza_netti = giorni_permanenza_lordi;
run;
data out1;
	set out1;

	if giorni_permanenza_netti<0 then
		giorni_permanenza_netti=0;
run;

/*
selezionare le valutazioni e le variabili collegate
*/
data valutazioni; set m1 
(keep= 
codice_soggetto
id_valutazione 
data_domanda
data_valutazione 
anno_val
patologia_prevalente 
patologia_concomitante
patologia_concomitante_ii
area_funzionale
area_mobilita
area_cognitiva
area_disturbi_comp
svama_ris_dec_vpia
svama_ass_inf_vip
svama_pot_res_vpot
necessita_ass_san_vsan
punteggio_svama
); where data_valutazione ne . ;  run; 

/*CONTARE IL TOTALE DELLE VALUTAZIONI A PRESCINDERE */


/*tengo al massimo tre valutazioni all'anno quali? le ultime */
/*prima tolgo i duplicati per codice_soggetto e stessa data di valutazione*/
proc sort nodupkey data=valutazioni out=v;
	by codice_soggetto data_valutazione;
run;

/*faccio una chiave per codice_soggetto e anno di valutazione*/
data v;
	set v;
	k=cats(codice_soggetto, anno_val);
run;

proc sort data=v;
	by k descending data_valutazione;
run;

data v;
	set v;
	by k;

	if first.k then
		count=1;
	else count+1;
run;


/*tengo le prime tre valutazioni massimo all'anno*/
data v3;
	set v;
	where count <=3 ;
run;

proc sort  data=v3;
	by codice_soggetto  ;
run;


/*faccio una chiave per codice_soggetto (tolgo la prima valutazione di quelli che ne hanno fatte 10 */

proc sort data=v3;
	by codice_soggetto descending data_valutazione;
run;

data v3;
	set v3;
	by codice_soggetto;

	if first.codice_soggetto then
		count_dieci=1;
	else count_dieci+1;
run;

data v4; set v3; where count_dieci < 10; run;   

/*traspongo variabile per variabile per soggetto
	var data_valutazione 
		patologia_prevalente 
		patologia_concomitante
		patologia_concomitante_ii
		area_funzionale
		area_mobilita
		area_cognitiva
		area_disturbi_comp
		svama_ris_dec_vpia
		svama_ass_inf_vip
		svama_pot_res_vpot
		necessita_ass_san_vsan
		punteggio_svama
		data_domanda;
*/
proc transpose data=v4 out=data_valutazione_t prefix=data_valutazione ;
	var data_valutazione;
	by codice_soggetto;
run;
data data_valutazione_t; set data_valutazione_t; drop _name_ _label_; run; 
proc transpose data=v4 out=data_domanda_t prefix=data_domanda ;
	var data_domanda;
	by codice_soggetto;
run;
data data_domanda_t; set data_domanda_t; drop _name_ _label_; run; 
proc transpose data=v4 out=patologia_prevalente_t prefix=patologia_prevalente  ;
	var patologia_prevalente;
	by codice_soggetto;
run;
data patologia_prevalente_t; set patologia_prevalente_t; drop _name_ _label_; run; 
proc transpose data=v4 out=patologia_concomitante_t prefix=patologia_concomitante  ;
	var patologia_concomitante;
	by codice_soggetto;
run;
data patologia_concomitante_t; set patologia_concomitante_t; drop _name_ _label_; run; 
proc transpose data=v4 out=patologia_concomitanteii_t prefix=patologia_concomitanteii  ;
	var patologia_concomitante_ii;
	by codice_soggetto;
run;
data patologia_concomitanteii_t; set patologia_concomitanteii_t; drop _name_ _label_; run; 
proc transpose data=v4 out=area_funzionale_t prefix=area_funzionale ;
	var area_funzionale;
	by codice_soggetto;
run;
data area_funzionale_t; set area_funzionale_t; drop _name_ _label_; run; 
proc transpose data=v4 out=area_mobilita_t prefix=area_mobilita ;
	var area_mobilita;
	by codice_soggetto;
run;
data area_mobilita_t; set area_mobilita_t; drop _name_ _label_; run; 
proc transpose data=v4 out=area_cognitiva_t prefix=area_cognitiva  ;
	var area_cognitiva;
	by codice_soggetto;
run;
data area_cognitiva_t; set area_cognitiva_t; drop _name_ _label_; run; 
proc transpose data=v4 out=area_disturbi_comp_t prefix=area_disturbi_comp  ;
	var area_disturbi_comp;
	by codice_soggetto;
run;
data area_disturbi_comp_t; set area_disturbi_comp_t; drop _name_ _label_; run;
proc transpose data=v4 out=svama_ris_dec_vpia_t prefix=svama_ris_dec_vpia  ;
	var svama_ris_dec_vpia;
	by codice_soggetto;
run;
data svama_ris_dec_vpia_t; set svama_ris_dec_vpia_t; drop _name_ _label_; run;
proc transpose data=v4 out=svama_ass_inf_vip_t prefix=svama_ass_inf_vip ;
	var svama_ass_inf_vip;
	by codice_soggetto;
run;
data svama_ass_inf_vip_t; set svama_ass_inf_vip_t; drop _name_ _label_; run;
proc transpose data=v4 out=svama_pot_res_vpot_t prefix=svama_pot_res_vpot  ;
	var svama_pot_res_vpot;
	by codice_soggetto;
run;
data svama_pot_res_vpot_t; set svama_pot_res_vpot_t; drop _name_ _label_; run;
proc transpose data=v4 out=necessita_ass_san_vsan_t prefix=necessita_ass_san_vsan  ;
	var necessita_ass_san_vsan;
	by codice_soggetto;
run;
data necessita_ass_san_vsan_t; set necessita_ass_san_vsan_t; drop _name_ _label_; run;
proc transpose data=v4 out=punteggio_svama_t prefix=punteggio_svama  ;
	var punteggio_svama;
	by codice_soggetto;
run;
data punteggio_svama_t; set punteggio_svama_t; drop _name_ _label_; run;
/*
bisogna fare i merge per codice soggetto
*/
proc sort data	= 		data_valutazione_t; by codice_soggetto; run; 
proc sort data	= 		data_domanda_t; by codice_soggetto; run; 
proc sort data	= 		patologia_prevalente_t; by codice_soggetto; run; 
proc sort data	= 		patologia_concomitante_t; by codice_soggetto; run; 
proc sort data	= 		patologia_concomitanteii_t; by codice_soggetto; run; 
proc sort data	= 		area_funzionale_t; by codice_soggetto; run; 
proc sort data	= 		area_mobilita_t; by codice_soggetto; run; 
proc sort data	= 		area_cognitiva_t; by codice_soggetto; run; 
proc sort data	= 		area_disturbi_comp_t; by codice_soggetto; run; 
proc sort data	= 		svama_ris_dec_vpia_t; by codice_soggetto; run; 
proc sort data	= 		svama_ass_inf_vip_t; by codice_soggetto; run; 
proc sort data	= 		svama_pot_res_vpot_t; by codice_soggetto; run; 
proc sort data	= 		necessita_ass_san_vsan_t; by codice_soggetto; run; 
proc sort data	= 		punteggio_svama_t; by codice_soggetto; run; 

data val_long;
	merge 
		data_valutazione_t (in=a)
		patologia_prevalente_t (in=b) 
		patologia_concomitante_t (in=c)
		patologia_concomitanteii_t (in=d)
		area_funzionale_t (in=e)
		area_mobilita_t (in=f)
		area_cognitiva_t (in=g)
		area_disturbi_comp_t (in=h) 
		svama_ris_dec_vpia_t (in=i)
		svama_ass_inf_vip_t (in=j)
		svama_pot_res_vpot_t (in=k)
		necessita_ass_san_vsan_t (in=l)
		punteggio_svama_t (in=m)
		data_domanda_t (in=n);

	if a;
	by codice_soggetto;
run;
data out5; set val_long; run; 
/*
comune di residenza al primo ingresso
livello all'entrata 
*/
data out2;
	set m1; 
	where count_date_in=1;
run;
data out2; set out2  ; 
keep comune_res_in azienda_res_in tipo_udo codice_soggetto;run; 
data out2; set out2  ; 
rename tipo_udo=tipo_udo_in; run; 
/*
il soggetto ha mai avuto un'impegnativa?
definisco un soggetto con flag_impegnativa_ever se ha compilato uno di questi campi:
numero_impegnativa
data_impegnativa
**/
data imp; set m1; run;
data imp; set imp; 
if num_impegnativa ne . or data_impegnativa ne .  then 
flag_impegnativa=1; else flag_impegnativa=0; run;  

data imp; set imp; keep codice_soggetto flag_impegnativa; run; 

data imp0; set imp; if flag_impegnativa=0; run; 
data imp1; set imp; if flag_impegnativa=1; run; 

proc sort nodupkey data=imp0; by codice_soggetto; run; 
proc sort nodupkey data=imp1; by codice_soggetto; run; 

data imp_ok; merge imp0 (in=a) imp1 (in=b) ; by codice_soggetto; 
if a and not b then flag_impegnativa_ever=0;
if b and not a then flag_impegnativa_ever=1; 
if a and b then flag_impegnativa_ever=1; 
run; 

data out3 (keep =  codice_soggetto flag_impegnativa_ever);
	set imp_ok;
run;

/*
flag cambio livello se nella presa in carico cambia livello (1: 1->2, 2: 2->1 )
data cambio livello 
*/

data livello; set m1; keep codice_soggetto data_ingresso_udo tipo_udo; run; 
proc sort data=livello; by codice_soggetto data_ingresso_udo tipo_udo; run ; 
data lag_difg;
	set livello;
	by codice_soggetto data_ingresso_udo tipo_udo;
 tipo_dif = dif (tipo_udo);

	if  first.codice_soggetto then
		do;
			tipo_dif=.;

		end;
run;
data lag_ok; set lag_difg; where tipo_dif in (1 -1) ; run; 
/*
tengo solo l'ultimo cambio livello in ordine di data ingresso udo
*/
proc sort data=lag_ok ; by codice_soggetto descending data_ingresso_udo; run; 
data lag_ok;
	set lag_ok;
	by codice_soggetto;

	if first.codice_soggetto then
		c=1;
	else c=0;
run;

data out4 (keep= codice_soggetto data_ingresso_udo tipo_dif); set lag_ok; where c=1; run;  
data out4; set out4 ; rename data_ingresso_udo = data_cambio_livello; run; 
data out4; set out4; 
if tipo_dif=-1 then flag_cambio_livello='2->1';
if tipo_dif=1 then  flag_cambio_livello='1->2';
run; 
data out4; set out4; drop tipo_dif; run; 
/*variabili anagrafiche*/
data out6;
	set m1;
	keep 
		codice_soggetto sesso data_nascita COD_STRUT_EROG_UDO azienda_erogatrice
stato_civile titolo_studio provenienza eta_ingresso data_ingresso_udo data_dimissione;
run;

proc sort nodupkey data=out6; by codice_soggetto data_ingresso_udo; run; 

data out6;
	set out6;
	by codice_soggetto;

	if first.codice_soggetto then
		cc=1;
	else cc+1;
run;

data out6; set out6; where cc=1; run; 
data out6; set out6; drop cc; run; 

/*merge*/
proc sort data=out6; by codice_soggetto; run; /*anagrafica*/
proc sort data=out5; by codice_soggetto; run; /*valutazioni*/
proc sort data=out4; by codice_soggetto; run; /*cambio livello*/
proc sort data=out3; by codice_soggetto; run; /*impegnativa*/
proc sort data=out2; by codice_soggetto; run; /*tipo udo e comune di residenza all'entrata*/
proc sort data=out1; by codice_soggetto; run; /*giorni di permanenza data primo ingresso data ultima dimissione*/

data coorte_far_2015_2017; merge 
out6 (in=a)
out5 (in=b)
out4 (in=c)
out3 (in=d)
out2 (in=e)
out1 (in=f)
;
run; 

data coorte_far_2015_2017; set coorte_far_2015_2017;
keep 
codice_soggetto
COD_STRUT_EROG_UDO
tipo_udo_in
sesso 
data_nascita 
stato_civile 
titolo_studio 
eta_ingresso 
data_domanda1-data_domanda9
data_valutazione1-data_valutazione9
data_primo_ingresso_udo
data_ultima_dimissione
data_dimissione
giorni_permanenza_netti
data_ingresso_udo 
data_dimissione
data_ultima_dimissione
flag_impegnativa_ever
azienda_res_in
azienda_erogatrice
comune_res_in
provenienza
patologia_prevalente1-patologia_prevalente9 
patologia_concomitante1-patologia_concomitante9
patologia_concomitanteii_1-patologia_concomitanteii_9
area_funzionale1-area_funzionale9
area_mobilita1-area_mobilita9
area_cognitiva1-area_cognitiva9
area_disturbi_comp1-area_disturbi_comp9
svama_ris_dec_vpia1-svama_ris_dec_vpia9
svama_ass_inf_vip1-svama_ass_inf_vip9
svama_pot_res_vpot1-svama_pot_res_vpot9
necessita_ass_san_vsan1-necessita_ass_san_vsan9
punteggio_svama1-punteggio_svama9
data_cambio_livello
flag_cambio_livello
; 
run; 
/*
check su duplicati
proc sort nodupkey data=coorte_far_2015_2017 out=ppp; by codice_soggetto; run; 
*/
/*
formati 
*/
proc format;
	value $provenienza 		
		'1'='1-Abitazione'
		'3'='3-Struttura sociale'
		'4'='4-Struttura ospedaliera'
		'5'='5-Struttura ricovero intermedia'
		'6'='6-Cambio livello assistenziale'
		'7'='7-Trasferimento da altra UDO - senza cambio livello'
		'8'='8-Apertura amministrativa per riassetto territoriale ULSS'
		'9'='9-Altro';
	value	$SEX
		'1'='Maschio'
		'2'='Femmina'
		'9'='Non disponibile';
	VALUE $STATO_CIVILE
		'1'='Celibe/Nubile'
		'2'='Coniugato'
		'3'='Separato'
		'4'='Divorziato'
		'5'='Vedovo'
		'9'='Non dichiarato';
	VALUE $STUDIO
		'1'='Nessuno'
		'2'='Licenza Elementare'
		'3'='Licenza Media Inferiore'
		'4'='Diploma Media Superiore'
		'5'='Diploma Universitario'
		'6'='Laurea'
		'7'='Scuola Professionale'
		'9'='Sconosciuto';
	VALUE flag
		1='SI'
		0='NO';
	value tipo_udo_short
		1='I LIVELLO'
		2='II LIVELLO';
run;

data coorte_far_2015_2017;
	set coorte_far_2015_2017;
	format provenienza provenienza.
		sesso sex.
		stato_civile stato_civile.
		titolo_studio studio.
		flag_impegnativa_ever flag.
		tipo_udo_in tipo_udo_short.

	;
run;
/*
/* appunti: 
tengo un solo record per soggetto 
con la data di accesso inferiore, data di dimissione maggiore 
e conteggio dei giorni di presenza
diagnosi le tengo tutte per riga 
numero valutazioni massimo 3 all'anno
data valutazione 1 2 3 
residenza al primo ingresso
livello all'entrata 
flag cambio livello se nella presa in carico cambia livello (1: 1->2, 2: 2->1 )
data cambio livello 
flag_impegnativa (1 se ho avuto l'imp almeno una volta)
*/