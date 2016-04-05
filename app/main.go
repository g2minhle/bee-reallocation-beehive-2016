package main

import (
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"

    "github.com/kandoo/beehive"
    "github.com/kandoo/beehive/Godeps/_workspace/src/github.com/gorilla/mux"
    "github.com/kandoo/beehive/Godeps/_workspace/src/golang.org/x/net/context"
)

// The tructure for logging information
type Logger struct {
    Trace   *log.Logger
    Info    *log.Logger
    Warning *log.Logger
    Error   *log.Logger
}

// Init the logger
//
// Agrs:
//      traceHandle (io.Writer): IO writer for Trace logging
//      infoHandle (io.Writer): IO writer for Info logging
//      warningHandle (io.Writer): IO writer for Warning logging
//      errorHandle (io.Writer): IO writer for Error logging
//
func (currentLogger *Logger) Init(
                                   traceHandle io.Writer,
                                   infoHandle io.Writer,
                                   warningHandle io.Writer,
                                   errorHandle io.Writer) {
    currentLogger.Trace = log.New(traceHandle,
        "TRACE: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    currentLogger.Info = log.New(infoHandle,
        "INFO: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    currentLogger.Warning = log.New(warningHandle,
        "WARNING: ",
        log.Ldate|log.Ltime|log.Lshortfile)

    currentLogger.Error = log.New(errorHandle,
        "ERROR: ",
        log.Ldate|log.Ltime|log.Lshortfile)
}

// Init the logger with default configuration
//
func (logger *Logger) InitDefault() {
    logger.Init(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
}

var (
    logger Logger
)

// Calculate a Fibonacci numbers
//
// Args
//      i (int): The index of the Fibonacci number
//
// Return
//      The i th Fibonacci number
//
func Fib (i int) int {
    if (i == 1 || i == 0){
        return 1;
    } else {
        return Fib(i - 1) + Fib(i - 2)
    }
}

// The message to send to a certain bee
type MessageToBee struct {
    DestinationBee string
    FibNumber int
}

// The bee hander
func BeeHandler(
                 beehiveMessage beehive.Msg,
                 beeContext beehive.RcvContext) error {
    // beehiveMessage is an envelope around the Hello message.
    // You can retrieve the Hello, using msg.Data() and then
    // you need to assert that its a MessageToBee.
    message := beehiveMessage.Data().(MessageToBee)
    // Using ctx.Dict you can get (or create) a dictionary.
    dict := beeContext.Dict("beehive-app-dict")
    value, err := dict.Get(message.DestinationBee)

    logger.Trace.Printf("[BeeHandler] Message sent to bee with id (%s) \n",
                         message.DestinationBee)

    count := 0
    if err == nil {
        // No error mean there is already an item with given key
        count = value.(int)
    }
    count++
    logger.Trace.Printf("[BeeHandler] Count = %d\n",
                         count)

    logger.Trace.Printf("[BeeHandler] Calculate fib number %d\n",
                         message.FibNumber)
    Fib(message.FibNumber)

    beeContext.Reply(beehiveMessage, count)
    return dict.Put(message.DestinationBee, count)
}

type HTTPReceiver struct{}

func BadRequest(responseWriter http.ResponseWriter, message string) {
    logger.Warning.Println("[HTTPReceiver] " + message)
    http.Error(responseWriter, message, http.StatusBadRequest)
}

func (httpReceiver *HTTPReceiver) ServeHTTP(
                                             responseWriter http.ResponseWriter,
                                             httpRequest *http.Request) {
    logger.Info.Printf("[HTTPReceiver] ServeHTTP %s %s \n",
                        httpRequest.Method,
                        httpRequest.URL)
    vars := mux.Vars(httpRequest)
    destinationBee, ok := vars["destinationBee"]
    if !ok {
        BadRequest(responseWriter, "No destinationBee")
        return
    }

    fibNumberStr, ok := vars["fibNumber"]
    if !ok {
        BadRequest(responseWriter, "No fibNumber")
        return
    }

    fibNumber, err := strconv.Atoi(fibNumberStr)
    if err != nil {
        BadRequest(responseWriter, "FibNumber must be number")
        return
    }

    message := MessageToBee{
        DestinationBee: destinationBee,
        FibNumber: fibNumber,
    }
    logger.Trace.Printf("[HTTPReceiver] Message to bee %+v \n", message)

    beeRespond, err := beehive.Sync(context.TODO(), message)
    if err != nil {
        logger.Error.Printf("[HTTPReceiver] %s \n", err.Error())
        http.Error(responseWriter, err.Error(), http.StatusInternalServerError)
        return
    }

    fmt.Fprintf(responseWriter, "%d", beeRespond.(int))
    logger.Trace.Println("[HTTPReceiver] Done sending message to bee")
}

// Register the HTTP handler
//
// Args
//     beehiveApp (beehive.App): A beehive app instance
//
func initHTTPHandler(beehiveApp beehive.App) {
    logger.Trace.Println("[main] Init HTTP Handler")
    beehiveApp.HandleHTTP("/{destinationBee}/{fibNumber}",
                           &HTTPReceiver{}).Methods("POST")
}

func main() {
    logger.InitDefault()
    logger.Trace.Println("[main] Init beehive app")
    // Create the application and
    beehiveApp := beehive.NewApp("beehive-app", beehive.Persistent(0))
    // Register the handler for MessageToBee messages.
    beehiveApp.HandleFunc(
                           MessageToBee{},
                           beehive.RuntimeMap(BeeHandler),
                           BeeHandler)

    initHTTPHandler(beehiveApp)

    logger.Trace.Println("[main] Start beehive")
    beehive.Start()
}
