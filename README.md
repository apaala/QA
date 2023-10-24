# QA
QA script for checking submissions


Current Status:

Tested only on submission: ypw4skw

Following techniques are ready to be tested:
-10X Genomics Multiome;RNAseq
-10X Genomics Immune profiling;VDJ
-10X Genomics Immune profilling;GEX
-10xv2
-10xv3
-10xmultiome_cell_hash;RNA

Implemented and not tested:
-10X Genomics Multiome;ATAC-seq
-10xmultiome_cell_hash;ATAC

NOTE: Not ready to be used by itself, just for testing as we figure out if its doing the right thing.
Error logs should report pass/fail

I have added print statements to make it easier to determine where it failed/ an issue is.

--------------------------------------------------------------------------------------------------
Usage:

Step 1
-------
Clone repo:
	Go to folder you want to put script in
command:
	git clone https://github.com/apaala/QA.git

Step 2
-------
Load python 
command:
	module load python/3.8


Step 3
------
Test repo

Print usage:
	python path/2/script/QA.py -h

Step 4
--------
Launch on grid:

Recommend using on grid, as md5sum checking can be time consuming.
Ex. 
	qsub -cwd -b y -l mem_free=4G -P owhite-irc-ffs -q all.q -e testx.err -o testx.out -V python QA.py -d /path/to/dir//ypw4skw/ -m /manifest/ypw4skw_manifest.tsv -t tech.csv

*****Rename testx.err and testx.out to track your logs correctly. Recommend using something unique for every submission
*****Use full paths.
*****Not ready for production!

