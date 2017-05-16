#!/bin/bash

export PERL5LIB=/root/perl5/lib/perl5

#parametri di collegamento db mysql
IP='10.11.14.78'
USERNAME='root'
PASSWORD='mela'

DESTINATARI='marco.gnecchi@if65.it'

DATA=$(date "+DATA: %d/%m/%Y ORA: %H:%M:%S")

#verifico che non ci siano import da eseguire
STMT='select count(*) from log.dc_epipoli_coda_esecuzione where stato = 0;'
#eseguo la query e salvo il risultato
IMPORT_IN_ESECUZIONE=$(mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT")
if [ $IMPORT_IN_ESECUZIONE -eq "0" ]; then
	
	#creo un log import
	STMT='insert into log.dc_epipoli_coda_esecuzione set tipo = 1;'
	mysql -u $USERNAME -p$PASSWORD -h $IP -ss -e "$STMT"
	
fi