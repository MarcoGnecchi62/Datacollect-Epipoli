#!/bin/bash

export PERL5LIB=/root/perl5/lib/perl5

#parametri di collegamento db mysql
IP='localhost'
USERNAME='root'
PASSWORD='mela'

#parametri di collegamento FTP
FTP_IP='itm-lrp01.italmark.com'
FTP_USERNAME="lrp"
FTP_PASSWORD="lrp"

#parametri di collegamento FTP Catalina
FTP_CATALINA_IP='ftp.catalinamarketing.it'
FTP_CATALINA_USERNAME="italmark"
FTP_CATALINA_PASSWORD="it3452rk"

#posizione dei datacollect ncr
DIR_BASE=/dati/datacollect

#posizione cartelle di preparazione dati per l'invio
DIR_PREPARAZIONE="/preparazione"
DIR_DC_DA_INVIARE="$DIR_PREPARAZIONE/da_inviare"
DIR_DC_INVIATI="$DIR_PREPARAZIONE/inviati"
DIR_DC_ERRATI="$DIR_PREPARAZIONE/dc_errati"
DIR_LAVORO_CATALINA="$DIR_PREPARAZIONE/file_catalina"
DIR_DC_DA_INVIARE_CATALINA="$DIR_PREPARAZIONE/da_inviare_catalina"
DIR_DC_INVIATI_CATALINA="$DIR_PREPARAZIONE/inviati_catalina"
DIR_DC_ERRATI_CATALINA="$DIR_PREPARAZIONE/dc_errati_catalina"

#nomi dei file log errori
LOG_ERRORI_GLOBALE="$DIR_PREPARAZIONE/errori_globali.txt"
LOG_ERRORI_NEGOZIO="$DIR_PREPARAZIONE/errori_negozio.txt"

#creo le cartelle di preparazione
mkdir -p $DIR_PREPARAZIONE
mkdir -p $DIR_DC_DA_INVIARE
mkdir -p $DIR_DC_INVIATI
mkdir -p $DIR_DC_ERRATI
mkdir -p $DIR_LAVORO_CATALINA
mkdir -p $DIR_DC_DA_INVIARE_CATALINA
mkdir -p $DIR_DC_INVIATI_CATALINA
mkdir -p $DIR_DC_ERRATI_CATALINA

#ricezione dati
perl /script/ricezione_dati/ricezione_datacollect.pl

rm -f $DIR_DC_ERRATI/*
rm -f /italmark/etl/ncr/datacollect/hocidc/last/*
rm -f /italmark/etl/epipoli/file/*

#query per ottenere i nomi delle cartelle che contengono i file da trasformare in dc epipoli
STMT='	select distinct date_format(i.`data`,"'"%Y%m%d"'") \
		from lavori.incarichi as i join \
		(select `negozio_codice`, `data` from lavori.incarichi where `lavoro_codice`=100 and `eseguito`=0) as h on i.`negozio_codice`=h.`negozio_codice` and i.`data`=h.`data` \
		where i.`lavoro_codice`=10 and i.`eseguito`=1 \
		order by 1;'
#eseguo la query e salvo il risultato
LISTA_CARTELLE=$(mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT")

#ora elimino i file *.ERR contenuti nelle cartelle selezionate che sono uguali al loro corrispondente file *.TXT
#questa cancellazione si rende necessaria perché il file *.ERR potrebbe essere creato per cause esterne (Es. mancanza di una promozione sul cm)
#elimino anche i file il cui corrispondente CTL sia ancora presente perché potrebbero essere incompleti.
for d in $LISTA_CARTELLE; do
	LISTA_FILE_ERR=$(cd $DIR_BASE/$d;ls -1 *.ERR 2>/dev/null)
	if [ -n "$LISTA_FILE_ERR" ]
	then
		for f in $LISTA_FILE_ERR; do
			TEST=$(diff -q $DIR_BASE/$d/$f $DIR_BASE/$d/${f/.ERR/.TXT})
			if [ -z "$TEST" ]
			then
				rm -f $DIR_BASE/$d/$f
			fi
		done
	fi
	LISTA_FILE_CTL=$(cd $DIR_BASE/$d;ls -1 *.CTL 2>/dev/null)
	if [ -n "$LISTA_FILE_CTL" ]
	then
		for f in $LISTA_FILE_CTL; do
			rm -f $DIR_BASE/$d/${f/.CTL/*}
		done
	fi
done

#query per ottenere i nomi dei file da trasformare in dc epipoli
STMT='	select concat(i.`negozio_codice`,"'"_"'",date_format(i.`data`,"'"%Y%m%d"'"),"'"_"'",date_format(i.`data`,"'"%y%m%d"'"),"'"_DC.TXT"'") \
		from lavori.incarichi as i join \
		(select `negozio_codice`, `data` from lavori.incarichi where `lavoro_codice`=100 and `eseguito`=0) as h on i.`negozio_codice`=h.`negozio_codice` and i.`data`=h.`data` \
		where i.`lavoro_codice`=10 and i.`eseguito`=1 \
		order by 1;'
#eseguo la query e salvo il risultato
LISTA_FILE=$(mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT")

#creo il file errori globale
>$LOG_ERRORI_GLOBALE

#preparo singolarmente ogni negozio
for f in $LISTA_FILE; do
	NEGOZIO=${f:0:4}
	DATA="${f:5:4}-${f:9:2}-${f:11:2}"
	printf "$(date +%Y-%m-%d:%H:%M:%S): elaborazione negozio $NEGOZIO, $DATA\n"
	
	>$LOG_ERRORI_NEGOZIO
	
	#verifico che l'ultima riga sia una :F:
	tail -n 1 $DIR_BASE/${f:5:8}/$f |grep -E '^[[:digit:]]'|grep -Ev ':F:' >> $LOG_ERRORI_NEGOZIO
	
	cp $DIR_BASE/${f:5:8}/$f /italmark/etl/ncr/datacollect/hocidc/last
	#verifico che non ci siano transazione di valore diverso da 0 con data errata
	grep -rhvE "^....:...:${f:7:6}:" /italmark/etl/ncr/datacollect/hocidc/last | grep -E ':F:1' |grep -Ev '^.{69}0{9}' >> $LOG_ERRORI_NEGOZIO
	
	mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "delete from ncr.negozio" 2>/dev/null
	mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "insert into ncr.negozio (neg_ncr,neg_itm) values('${f:0:4}','${f:0:4}')" 2>/dev/null
	perl /script/01_sistemazione_iniziale_file_ncr.pl /italmark/etl/ncr/datacollect/hocidc/last 1>/dev/null
	perl /script/02_caricamento.pl 1>/dev/null
	perl /script/03_analisi.pl >> $LOG_ERRORI_NEGOZIO
	perl /script/04_da_ncr_a_epipoli.pl 1>/dev/null
	perl /script/05_da_ncr_a_catalina.pl #1>/dev/null
	perl /script/06_controllo_finale_file_epipoli.pl /italmark/etl/epipoli/file
	
	#verifico che le promozioni siano correttamente valorizzate
	grep -rhE '^.{20}[[:space:]]1[[:digit:]](00|08|13|51|77|85|86|89|90|91|93|94).{37}[[:space:]]{5}' /italmark/etl/epipoli/file >> $LOG_ERRORI_NEGOZIO
	
	#se non ci sono errori sposto il file nella cartella di invio
	if [ -s $LOG_ERRORI_NEGOZIO ]
	then
		printf "\- ${f:0:4}\n" >> $LOG_ERRORI_GLOBALE
		cat $LOG_ERRORI_NEGOZIO >> $LOG_ERRORI_GLOBALE
		mv /italmark/etl/epipoli/file/* $DIR_DC_ERRATI
		mv $DIR_LAVORO_CATALINA/* $DIR_DC_ERRATI_CATALINA
		if [ ! -f $DIR_BASE/${f:5:8}/${f/.TXT/.ERR} ]
		then
			cp $DIR_BASE/${f:5:8}/$f $DIR_BASE/${f:5:8}/${f/.TXT/.ERR}
		fi
		
		# carico gli scontrini nella tabella del dc di controllo
		perl /script/carica_dc_ncr.pl -s $NEGOZIO -d $DATA -p $DIR_BASE/${f:5:4}${f:9:2}${f:11:2}	
	else
		#esecuzione x statistiche
		perl /it/bin/SQL_List.pl -s STATISTICHE_DC
		
		#venduto articolo
		perl /it/bin/SQL_File.pl -f /script/statistiche_dc/venduto_articolo.sql
	
		#sposto il dc epipoli nella cartella di invio
  		mv /italmark/etl/epipoli/file/* $DIR_DC_DA_INVIARE
  		#sposto il dc catalina nella cartella di invio
  		mv $DIR_LAVORO_CATALINA/* $DIR_DC_DA_INVIARE_CATALINA
	fi
	
	rm -f /italmark/etl/ncr/datacollect/hocidc/last/*;
done

#se ci sono file epipoli da inviare li invio
cd $DIR_DC_DA_INVIARE

for f in $(ls *.DAT)
do
	SOCIETA=${f:11:2}
	NEGOZIO=${f:11:4}
	DATA="${f:2:4}-${f:6:2}-${f:8:2}"
	
	CARTELLA_INVIO_FTP="/PO_fid_files/trasm/000$SOCIETA/IN"
	
	FILE_CONTROLLO=${f/.DAT/.CTL}
	>$FILE_CONTROLLO
	
	ftp -in $FTP_IP 1>/dev/null <<SCRIPT
		user $FTP_USERNAME $FTP_PASSWORD
		binary
		lcd $DIR_DC_DA_INVIARE
		cd $CARTELLA_INVIO_FTP
		put $f
		put $FILE_CONTROLLO
		bye
SCRIPT
	mv $DIR_DC_DA_INVIARE/$f $DIR_DC_INVIATI
	rm -f $DIR_DC_DA_INVIARE/$FILE_CONTROLLO
	
	mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "update lavori.incarichi set eseguito=1 where lavoro_codice=100 and negozio_codice='$NEGOZIO' and data='$DATA';" 2>/dev/null
	perl /script/carica_dc_epipoli.pl -s $NEGOZIO -d $DATA -p $DIR_DC_INVIATI
done

# se ci sono file catalina da inviare li invio
cd $DIR_DC_DA_INVIARE_CATALINA
if [ "$(ls -A $DIR_DC_DA_INVIARE_CATALINA/al.it.2??.00*.zip 2>/dev/null)" ]
then
	ftp -in $FTP_CATALINA_IP 1>/dev/null <<SCRIPT
	user $FTP_CATALINA_USERNAME $FTP_CATALINA_PASSWORD
	binary
	mput al.it.268.00*.zip
	mput al.it.268.01*.zip
	mput al.it.271.16*.zip
	mput al.it.271.36*.zip
	bye
SCRIPT
	for f in $(ls *.zip)
	do
		case "${f:10:2}" in
			"00")	SOCIETA="01";;
			"16")	SOCIETA="36";;
		esac
		
		case "${f:10:4}" in
			"0063")	SOCIETA="04";;
		esac
		
		NEGOZIO="$SOCIETA${f:12:2}"
		DATA="${f:19:4}-${f:23:2}-${f:25:2}"
		case "$NEGOZIO" in
			"0151")	NEGOZIO="3151";;
			"0152")	NEGOZIO="3152";;
		esac
		
		mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "update lavori.incarichi set eseguito=1 where lavoro_codice=110 and negozio_codice='$NEGOZIO' and data='$DATA';" 2>/dev/null
	done
	mv $DIR_DC_DA_INVIARE_CATALINA/*.zip $DIR_DC_INVIATI_CATALINA
fi

mysql -u root -pmela < /script/totali_ncr.sql
