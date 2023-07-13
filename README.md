# vbi-smk  
Snakemake pipeline to run [VerifyBamID](https://github.com/Griffan/VerifyBamID)  
VerifyBamID conducts a PCA based on illumina snps and estimates contamination based on allele frequencies at these sites compared to their predicted PC groups.  
  
To run the vbi-smk pipeline:  
  
1. Align reads to ref: /net/eichler/vol26/eee_shared/assemblies/hg38/no_alt/hg38.no_alt.fa  
2. Use config as-is.  
3. Create manifest with sample name and path to the bam files.  
4. Run:  
  
  
  
