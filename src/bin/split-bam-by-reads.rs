use anyhow::{Error, Ok, Result};
use clap::Parser;
use env_logger::{Builder, Target};
use indicatif::ProgressIterator;
use log::LevelFilter;
use rayon::prelude::*;
use rust_htslib::{bam, bam::Read};
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::path::PathBuf;

#[derive(Parser, Debug, PartialEq, Eq)]
#[clap(
    author,
    version,
    about,
    propagate_version = true,
    arg_required_else_help = true
)]
struct Opts {
    /// Threads for bam read and write
    #[clap(short, long, default_value = "16")]
    threads: usize,

    /// Input bam files
    #[clap()]
    bam: PathBuf,

    /// Input text files with read name to split bam on
    #[clap(short, long)]
    reads: Vec<PathBuf>,

    /// Logging level [-v: Info, -vv: Debug, -vvv: Trace]
    #[clap(
            global = true,
            short,
            long,
            action = clap::ArgAction::Count,
            help_heading = "Debug-Options"
        )]
    pub verbose: u8,
}

fn main() -> Result<(), Error> {
    let opts = Opts::parse();
    set_log_level(&opts);

    let reads = opts
        .reads
        .par_iter()
        .map(|path| {
            let file = File::open(path).unwrap();
            let reader = BufReader::new(file);
            let mut set = HashSet::new();
            for line in reader.lines() {
                set.insert(line.unwrap().trim().to_string());
            }
            let path_s = path.as_os_str().to_str().unwrap();
            log::info!("{} had {} reads", path_s, set.len());
            (path_s, set)
        })
        .collect::<Vec<_>>();

    // open bam
    let mut bam = bam::Reader::from_path(opts.bam)?;
    bam.set_threads(opts.threads)?;
    let header = bam::Header::from_template(bam.header());

    // make outputs
    let mut outs = Vec::new();
    for (path, set) in &reads {
        let out_path = Path::new(&path).with_extension("bam");
        let mut out = bam::Writer::from_path(out_path, &header, bam::Format::Bam)?;
        out.set_threads(opts.threads)?;
        outs.push((out, set.clone()));
    }
    // write results
    bam.records().progress_count(1).try_for_each(|record| {
        let record = record?;
        let query_name = std::str::from_utf8(record.qname())?;
        for (out, set) in &mut outs {
            if set.remove(query_name) {
                out.write(&record)?;
                break;
            }
        }
        Ok(())
    })?;

    // get unplaced
    for (_path, set) in outs {
        log::info!("had {} unplaced reads", set.len());
    }

    Ok(())
}

fn set_log_level(opts: &Opts) {
    // set the logging level
    let min_log_level = match opts.verbose {
        0 => LevelFilter::Warn,
        1 => LevelFilter::Info,
        2 => LevelFilter::Debug,
        _ => LevelFilter::Trace,
    };

    Builder::new()
        .target(Target::Stderr)
        .filter(None, min_log_level)
        .init();

    log::debug!("DEBUG logging enabled");
    log::trace!("TRACE logging enabled");
}
