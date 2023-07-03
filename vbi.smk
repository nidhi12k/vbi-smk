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
    sample="|".join(manifest_df.index)


localrules:
    all,


rule all:
    input:
        expand("{sample}/{sample}.selfSM", sample=manifest_df.index),


rule run_pileup:
    input:
        ref=REFERENCE,
        bed=get_bed,
        bam_file=get_bam,
    output:
        pileup="{sample}/{sample}.pile",
    threads: 1
    resources:
        mem=8,
        hrs=12,
    envmodules:
        "modules",
        "modules-init",
        "modules-gs/prod",
        "modules-eichler/prod",
        "samtools/1.17",
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
        selfsm="{sample}/{sample}.selfSM",
    resources:
        mem=20,
        hrs=1200,
    threads: 4
    params:
        svdprefix=SVDPREFIX,
    envmodules:
        "modules",
        "modules-init",
        "modules-gs/prod",
        "modules-eichler/prod",
        "vbi/2.0.1",
    shell:
        """
        VerifyBamID --PileupFile {input.pileup_files} --SVDPrefix {params.svdprefix} --NumThread {threads} --Reference {input.ref} --Output $( echo {output.selfsm} | sed 's/.selfSM//' )
        """
