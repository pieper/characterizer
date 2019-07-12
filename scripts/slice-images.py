
import dicomweb_client
import numpy
import os
import png
import pydicom
import subprocess

PROJECT_ID="chc-tcia"
REGION="us-central1"

DATASET_ID = subprocess.run(['curl', '--silent', 'http://metadata/computeMetadata/v1/instance/attributes/collection', '-H', 'Metadata-Flavor: Google'], stdout=subprocess.PIPE).stdout.decode().strip()
DICOM_STORE_ID=DATASET_ID

def freshClient():
    token = subprocess.run(['gcloud', 'auth', 'print-access-token'], stdout=subprocess.PIPE).stdout.decode().strip()
    url = f"https://healthcare.googleapis.com/v1beta1/projects/{PROJECT_ID}/locations/{REGION}/datasets/{DATASET_ID}/dicomStores/{DICOM_STORE_ID}/dicomWeb"
    headers = {
        "Authorization" : "Bearer %s" % token
    }
    return dicomweb_client.api.DICOMwebClient(url, headers=headers)


client=freshClient()
studies = client.search_for_studies()

print(f"\n{len(studies)} studies to query\n")

for study in studies:
  client=freshClient() # refresh token
  studyMetadata = dicomweb_client.api.load_json_dataset(study)
  if hasattr(studyMetadata, "ModalitiesInStudy"):
    print(f"Modalities: {studyMetadata.ModalitiesInStudy}")
  else:
    print("Modalities not defined")
  series = client.search_for_series(studyMetadata.StudyInstanceUID)
  print(f"  {len(series)} series")
  for serie in series:
    seriesMetadata = dicomweb_client.api.load_json_dataset(serie)
    instances = client.search_for_instances(
      study_instance_uid=studyMetadata.StudyInstanceUID,
      series_instance_uid=seriesMetadata.SeriesInstanceUID
    )
    print(f"    {len(instances)} instances ", end="", flush=True)
    directory = f'/mnt/extra/data/{DATASET_ID}'
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
  print()
