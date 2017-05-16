#!/bin/bash

export PERL5LIB=/root/perl5/lib/perl5

#parametri di collegamento db mysql
IP='10.11.14.78'
USERNAME='root'
PASSWORD='mela'

DESTINATARI='marco.gnecchi@if65.it'

#verifico che non ci siano import in esecuzione
STMT='select count(*) from log.dc_epipoli_coda_esecuzione where stato = 500;'
#eseguo la query e salvo il risultato
IMPORT_IN_ESECUZIONE=$(mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT")
if [ $IMPORT_IN_ESECUZIONE -eq "0" ]; then
	
	#verifico che ci siano import da eseguire
	STMT='select count(*) from log.dc_epipoli_coda_esecuzione where stato = 0;'
	#eseguo la query e salvo il risultato
	IMPORT_DA_ESEGUIRE=$(mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT")
	if [ $IMPORT_DA_ESEGUIRE -ne "0" ]; then
	
		#query per estrarre i parametri di esecuzione
		STMT='select id `` from log.dc_epipoli_coda_esecuzione where stato = 0 and tipo = 1 order by timestamp_apertura limit 1;'
					
		#eseguo la query e salvo il risultato
		COMANDO=$(mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT")

		#spezzo il comando nei parametri (per ora non ne uso)
		ID=$COMANDO
	
		#marco il record come "in esecuzione"
		STMT="update log.dc_epipoli_coda_esecuzione set stato=500 where id=$ID;"
		mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT"
		
		DATA=$(date "+DATA: %d/%m/%Y ORA: %H:%M:%S")
		BODY="<html><body>\n<b>INIZIO ELABORAZIONE DC EPIPOLI:$DATA<BR>$DATA</b><BR><BR>\n"
		SUBJECT="(ID:$ID) INIZIO ELABORAZIONE DC EPIPOLI"
		/sendEmail-v1.56/sendEmail -u $SUBJECT -m $BODY -f edp@if65.it -t $DESTINATARI -s 10.11.14.234:25 1>/dev/null 2>/dev/null
	
		/script/import_go.sh 1>/dev/null 2>/dev/null
		
		DATA=$(date "+DATA: %d/%m/%Y ORA: %H:%M:%S")
		BODY="<html><body>\n<b>FINE ELABORAZIONE DC EPIPOLI:$DATA<BR>$DATA</b><BR><BR>\n"
		SUBJECT="(ID:$ID) FINE ELABORAZIONE DC EPIPOLI "
		/sendEmail-v1.56/sendEmail -u $SUBJECT -m $BODY -f edp@if65.it -t $DESTINATARI -s 10.11.14.234:25 1>/dev/null 2>/dev/null

		#marco il record come "eseguito"
		STMT="update log.dc_epipoli_coda_esecuzione set stato=999, timestamp_chiusura = current_timestamp() where id=$ID;"
		mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT"
	fi
fi