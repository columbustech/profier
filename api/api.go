package main

import (
	"fmt"
	"net/http"
	"math/rand"
	"time"
	"strconv"
	"encoding/json"
	"strings"
	"os"
	"sync"
	"io/ioutil"
	"mime/multipart"
	"path/filepath"
	"bytes"
	apiv1 "k8s.io/api/core/v1"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

type WorkerSpecs struct {
	WorkerId int `json:"workerId"`
	TotalWorkers int `json:"nWorkers"`
	InputFolderPath string `json:"inputFolderPath"`
}

type JobDetails struct {
	sync.Mutex
	JobName string
	InputFolderPath string
	OutputFolderPath string
	TotalWorkers int
	RunningWorkers int
	MapperUrl string
	uid string
	imageUrl string
	completedWorkers int
	accessToken string
}

var jobDetailsMap map[string]*JobDetails

func main() {
	_ = os.Mkdir("/storage/output", 0755)
	jobDetailsMap = make(map[string]*JobDetails)
	http.HandleFunc("/create", createJob)
	http.HandleFunc("/init", initWorker)
	http.HandleFunc("/write-chunk", writeChunk)
	if err := http.ListenAndServe(":8000", nil); err != nil {
		panic(err)
	}
}

func generateUid() string {
	rand.Seed(time.Now().UnixNano())
	letters := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	s := make([]rune, 10)
	for i:= range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func createJob(w http.ResponseWriter, r* http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		authUrl := r.Header.Get("Authorization")
		tokens := strings.Split(authUrl, " ")
		accessToken := tokens[1]
		imageUrl := r.PostForm.Get("imageUrl")
		inputFolderPath := r.PostForm.Get("inputFolderPath")
		outputFolderPath := r.PostForm.Get("outputFolderPath")
		workers, _ := strconv.Atoi(r.PostForm.Get("workers"))
		uid := generateUid()
		jobName := "profilefunc-" + os.Getenv("COLUMBUS_USERNAME") + "-" + uid
		fmt.Printf("Created Job %s.\n", imageUrl)
		jobDetailsMap[uid] = &JobDetails{
			JobName: jobName,
			InputFolderPath: inputFolderPath,
			OutputFolderPath: outputFolderPath,
			TotalWorkers: workers,
			RunningWorkers: 0,
			MapperUrl: "http://profiler-" + os.Getenv("COLUMBUS_USERNAME") + "/",
			uid: uid,
			imageUrl: imageUrl,
			completedWorkers: 0,
			accessToken: accessToken,
		}
		createJobHelper(jobDetailsMap[uid])
		jobStatus(w, uid)
	}
}

func createJobHelper(jobDetails *JobDetails) {
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	nWorkers := int32(jobDetails.TotalWorkers)
	retries := int32(5)
	jobsClient := clientset.BatchV1().Jobs(apiv1.NamespaceDefault)
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: jobDetails.JobName,
		},
		Spec: batchv1.JobSpec{
			Template: apiv1.PodTemplateSpec{
				Spec: apiv1.PodSpec {
					Containers: []apiv1.Container{
						{
							Name: jobDetails.JobName,
							Image: jobDetails.imageUrl,
							Args: []string{"-u", jobDetails.MapperUrl, "-i", jobDetails.uid},
							Env: []apiv1.EnvVar{
								{
									Name: "COLUMBUS_ACCESS_TOKEN",
									Value: jobDetails.accessToken,
								},
							},

						},
					},
					RestartPolicy: apiv1.RestartPolicyNever,
				},
			},
			Completions: &nWorkers,
			Parallelism: &nWorkers,
			BackoffLimit: &retries,
		},
	}
	result, err := jobsClient.Create(job)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created Job %q.\n", result.GetObjectMeta().GetName())
}

func jobStatus(w http.ResponseWriter, uid string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	jobDetails := jobDetailsMap[uid]
	listOptions := metav1.ListOptions{}
	watcher, err := clientset.BatchV1().Jobs(apiv1.NamespaceDefault).Watch(listOptions)
	if err != nil {
		panic(err)
	}
	ch := watcher.ResultChan()
	for event := range ch {
		jobObject := event.Object.(*batchv1.Job)
		s, _ := json.Marshal(jobObject.Status)
		fmt.Fprintf(w, "%s\n", string(s))
		flusher.Flush()
		if conditions := jobObject.Status.Conditions; len(conditions) > 0 && conditions[0].Type == "Complete" {
			uploadToCDrive("/storage/output/" + uid + ".csv", jobDetails.OutputFolderPath, jobDetails.accessToken)
			break
		}
	}
}

func initWorker(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		uid := r.PostForm.Get("uid")
		w.Header().Set("Content-Type", "application/json")
		jobDetails := jobDetailsMap[uid]
		jobDetails.Lock()
		workerSpecs := &WorkerSpecs {
			WorkerId: jobDetails.RunningWorkers,
			TotalWorkers: jobDetails.TotalWorkers,
			InputFolderPath: jobDetails.InputFolderPath,
		}
		jobDetails.RunningWorkers++
		jobDetails.Unlock()
		json.NewEncoder(w).Encode(workerSpecs)
	}
}

func writeChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		if err := r.ParseMultipartForm(3 << 30); err != nil {
			http.Error(w, "Could not parse form.", http.StatusBadRequest)
			return
		}
		chunk, _ , err := r.FormFile("chunk")
		uid := r.PostForm.Get("uid")
		if err != nil {
			panic(err)
		}
		defer chunk.Close()
		chunkBytes, err := ioutil.ReadAll(chunk)

		filePath := "/storage/output/" + uid + ".csv"
		if _, err := os.Stat(filePath); err == nil {
			chunkBytes = chunkBytes[ bytes.IndexByte(chunkBytes, byte('\n')) + 1 : ]
		}
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		defer file.Close()
		file.Write(chunkBytes)
	}
}

func uploadToCDrive(localPath, cDrivePath, accessToken string) {
	url := "http://cdrive/multi-part-form-upload/"
	file, err := os.Open(localPath)
	if err != nil {
		panic(err)
	}
	fileContents, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}
	file.Close()

	body := new(bytes.Buffer)
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("file", filepath.Base(file.Name()))
	if err != nil {
		panic(err)
	}
	part.Write(fileContents)
	_ = writer.WriteField("path", cDrivePath)
	writer.Close()
	request, err := http.NewRequest("POST", url, body)
	if err != nil {
		panic(err)
	}
	request.Header.Add("Content-Type", writer.FormDataContentType())
	request.Header.Add("Authorization", "Bearer " + accessToken)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	response, err := client.Do(request)
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	_ , err = ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}
}
