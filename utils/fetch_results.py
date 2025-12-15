#!/usr/bin/env python3

import argparse
import pathlib
import openeo


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job_id")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    connection = openeo.connect(
        "https://openeo.dataspace.copernicus.eu/"
    ).authenticate_oidc()

    out_path = pathlib.Path(args.output_dir)
    out_path.mkdir(exist_ok=True, parents=True)
    job = connection.job(args.job_id)
    print(job.status())
    print(job.job_id)
    print(job.logs())
    results = job.get_results()
    print(results)
    results.download_files(out_path)


if __name__ == "__main__":
    main()
