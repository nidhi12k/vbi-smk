import pandas as pd
import os


configfile: "config.yaml"


MANIFEST = config.get("manifest", "manifest.tab")
SVDPREFIX = config["svd"]
REFERENCE = config["reference"]

SNAKEMAKE_DIR = os.path.dirname(workflow.snakefile)

manifest_df = pd.read_csv(MANIFEST, sep="\t", index_col="SAMPLE")


def get_bam(wildcards):
    return manifest_df.at[wildcards.sample, "BAM_PATH"]


def get_bed(wildcards):
    return f"{SVDPREFIX}.bed"


def check_svd_files(wildcards):
    return [f"{SVDPREFIX}.{x}" for x in ["bed", "mu", "UD", "V"]]


wildcard_constraints:
    sample="|".join(manifest_df.index),


localrules:
    all,
    vbi_summary,


rule all:
    input:
        "vbi_summary.tsv",


rule run_pileup:
    input:
        ref=REFERENCE,
        bed=get_bed,
        bam_file=get_bam,
    output:
        pileup="results/{sample}/{sample}.pile",
    threads: 1
    resources:
        mem=16,
        hrs=12,
    singularity:
        "docker://eichlerlab/binf-basics:0.1"
    shell:
        """
        samtools mpileup -B -f {input.ref} -l {input.bed} {input.bam_file} -o {output.pileup}
        """


rule run_vbi:
    input:
        pileup_files=rules.run_pileup.output.pileup,
        ref=REFERENCE,
        svdprefix=check_svd_files,
    output:
        selfsm="results/{sample}/{sample}.selfSM",
    resources:
        mem=8,
        hrs=1200,
    threads: 8
    params:
        svdprefix=SVDPREFIX,
    singularity:
        "docker://eichlerlab/vbi:2.0.1"
    shell:
        """
        VerifyBamID --PileupFile {input.pileup_files} --SVDPrefix {params.svdprefix} --NumThread {threads} --Reference {input.ref} --Output $( echo {output.selfsm} | sed 's/.selfSM//' )
        """


rule vbi_summary:
    input:
        sm_all=expand("results/{sample}/{sample}.selfSM", sample=manifest_df.index),
    output:
        vbi_summary="vbi_summary.tsv",
    shell:
        """
        head -1 {input.sm_all[0]} | cut -f 1,6-9 > {output.vbi_summary}
        for file in $( echo {input.sm_all} ); do sample=$(basename ${{file}} | awk -F "." '{{print $1}}' ); tail -n +2 ${{file}} | sed "s/DefaultSampleName/${{sample}}/" | cut -f 1,6-9; done >> {output.vbi_summary}
        """
