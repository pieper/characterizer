
import dicomweb_client
import numpy
import os
import png
import pydicom
import subprocess

notes = """

# be sure drive is installed
sudo apt-get install nfs-common
mount 10.51.62.154:/extra /mnt/extra/



"""

PROJECT_ID="chc-tcia"
REGION="us-central1"

completedCollections = ["qin-headneck", "4d-lung", "anti-pd-1_melanoma", "apollo", "breast-diagnosis", "breast-mri-nact-pilot",
    "cbis-ddsm", "cc-radiomics-phantom", "cptac-ccrcc", "cptac-cm", "cptac-gbm", "cptac-hnscc",
    "cptac-lscc", "cptac-luad", "cptac-pda", "cptac-ucec", "ct-colonography", "ct-lymph-nodes",
    "head-neck-cetuximab",
    "hnscc", "hnscc-3dct-rt", "ispy1", "ivygap",
    "lctsc", "lgg-1p19qdeletion", "lidc-idri", "lung-fused-ct-pathology", "lung-phantom",
    "lungct-diagnosis", "mouse-astrocytoma", "mouse-mammary", "mri-dir", "naf-prostate",
    "nsclc-radiogenomics", "nsclc-radiomics", "nsclc-radiomics-genomics", "pancreas-ct",
    "phantom-fda", 
    "prostate-3t", 
]

brokenCollections = [ "head-neck-pet-ct", ]

collections = [
    "prostate-diagnosis", "prostate-fused-mri-pathology", "prostatex",
    "qiba-ct-1c", "qin-breast-dce-mri", "qin-lung-ct", "qin-pet-phantom", "rembrandt",
    "rider-breast-mri", "rider-lung-ct", "rider-lung-pet-ct", "rider-neuro-mri", "rider-phantom-mri",
    "rider-phantom-pet-ct", "soft-tissue-sarcoma", "spie-aapm-lung-ct-challenge", "tcga-blca",
    "tcga-brca", "tcga-cesc", "tcga-coad", "tcga-esca", "tcga-gbm", "tcga-hnsc", "tcga-kich",
    "tcga-kirc", "tcga-kirp", "tcga-lgg", "tcga-lihc", "tcga-luad", "tcga-lusc", "tcga-ov",
    "tcga-prad", "tcga-read", "tcga-sarc", "tcga-stad", "tcga-thca", "tcga-ucec"
]

def freshClient(collection):
    token = subprocess.run(['gcloud', 'auth', 'print-access-token'], stdout=subprocess.PIPE).stdout.decode().strip()
    url = f"https://healthcare.googleapis.com/v1beta1/projects/{PROJECT_ID}/locations/{REGION}/datasets/{collection}/dicomStores/{collection}/dicomWeb"
    headers = {
        "Authorization" : "Bearer %s" % token
    }
    return dicomweb_client.api.DICOMwebClient(url, headers=headers)


for collection in collections:
    # DATASET_ID = subprocess.run(['curl', '--silent', 'http://metadata/computeMetadata/v1/instance/attributes/collection', '-H', 'Metadata-Flavor: Google'], stdout=subprocess.PIPE).stdout.decode().strip()
    # DICOM_STORE_ID=DATASET_ID


    client=freshClient(collection)
    studies = client.search_for_studies()

    print(f"\n{len(studies)} studies to query\n")

    studyCount = 0
    for study in studies:
      studyCount += 1
      client=freshClient(collection) # refresh token
      studyMetadata = dicomweb_client.api.load_json_dataset(study)
      print(f"\n{studyCount} of {len(studies)} studies\n")
      if hasattr(studyMetadata, "ModalitiesInStudy"):
        print(f"Modalities: {studyMetadata.ModalitiesInStudy}")
      else:
        print("Modalities not defined")
      series = client.search_for_series(studyMetadata.StudyInstanceUID)
      print(f"  {len(series)} series")
      try:
          for serie in series:
            seriesMetadata = dicomweb_client.api.load_json_dataset(serie)
            instances = client.search_for_instances(
              study_instance_uid=studyMetadata.StudyInstanceUID,
              series_instance_uid=seriesMetadata.SeriesInstanceUID
            )
            print(f"    {len(instances)} instances ", end="", flush=True)
            directory = f'/mnt/extra/data/{collection}'
            if not os.path.exists(directory):
              os.makedirs(directory)
              print(f'making {directory}')
            for instance in instances:
              instanceMetadata = dicomweb_client.api.load_json_dataset(instance)
              dataset = client.retrieve_instance(
                study_instance_uid=studyMetadata.StudyInstanceUID,
                series_instance_uid=seriesMetadata.SeriesInstanceUID,
                sop_instance_uid=instanceMetadata.SOPInstanceUID
              )
              print('.', end="", flush=True)
              try:
                shape = dataset.pixel_array.shape
                image_2d = dataset.pixel_array.astype(float)
                image_2d_scaled = (numpy.maximum(image_2d,0) / image_2d.max()) * 255.0
                image_2d_scaled = numpy.uint8(image_2d_scaled)
                destination = f'{directory}/{instanceMetadata.SOPInstanceUID}.png'
                with open(destination, 'wb') as png_file:
                  w = png.Writer(shape[1], shape[0], greyscale=True)
                  w.write(png_file, image_2d_scaled)
              except AttributeError:
                print(f'Skipping {instanceMetadata.SOPInstanceUID}')
      except OSError:
        print(f'Skipping series {serie} due to bad tag')
      except TypeError:
        print(f'Skipping series {serie} due to type error')
      print()
