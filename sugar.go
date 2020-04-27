// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package zap

import (
	"fmt"
	"strconv"

	"context"
	"encoding/json"

	"log"
	"time"

	"github.com/1209490572/goog/zapcore"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"

	"github.com/shijuvar/gokit/examples/nats-streaming/pb"
	"go.uber.org/multierr"
)

//SerName 服务名
var SerName string

func Getkey(appkey string, appsercet string) error {
	var tableName string
	cluster := gocql.NewCluster("118.24.5.107")
	cluster.Keyspace = "test"
	cluster.Consistency = 1
	session, _ := cluster.CreateSession()
	//设置连接池的数量,默认是2个（针对每一个host,都建立起NumConns个连接）
	cluster.NumConns = 3

	iter := session.Query(`SELECT table_name FROM congruentrelationship WHERE app_key = ? AND app_secret = ? allow filtering`, appkey, appsercet).Iter()
	for iter.Scan(&tableName) {
	}

	if err := iter.Close(); err != nil {
		log.Println(err, "查询出错")
		return err
	}
	SerName = tableName

	return nil
}

const (
	_oddNumberErrMsg    = "Ignored key without a value."
	_nonStringKeyErrMsg = "Ignored key-value pairs with non-string keys."
	channel             = "order-notification"
	event               = "OrderCreated"
	aggregate           = "order"
	grpcUri             = "127.0.0.1:50051"
)

// A SugaredLogger wraps the base Logger functionality in a slower, but less
// verbose, API. Any Logger can be converted to a SugaredLogger with its Sugar
// method.
//
// Unlike the Logger, the SugaredLogger doesn't insist on structured logging.
// For each log level, it exposes three methods: one for loosely-typed
// structured logging, one for println-style formatting, and one for
// printf-style formatting. For example, SugaredLoggers can produce InfoLevel
// output with Infow ("info with" structured context), Info, or Infof.
type SugaredLogger struct {
	base *Logger
}

// Desugar unwraps a SugaredLogger, exposing the original Logger. Desugaring
// is quite inexpensive, so it's reasonable for a single application to use
// both Loggers and SugaredLoggers, converting between them on the boundaries
// of performance-sensitive code.
func (s *SugaredLogger) Desugar() *Logger {
	base := s.base.clone()
	base.callerSkip -= 2
	return base
}

// Named adds a sub-scope to the logger's name. See Logger.Named for details.
func (s *SugaredLogger) Named(name string) *SugaredLogger {
	return &SugaredLogger{base: s.base.Named(name)}
}

// With adds a variadic number of fields to the logging context. It accepts a
// mix of strongly-typed Field objects and loosely-typed key-value pairs. When
// processing pairs, the first element of the pair is used as the field key
// and the second as the field value.
//
// For example,
//   sugaredLogger.With(
//     "hello", "world",
//     "failure", errors.New("oh no"),
//     Stack(),
//     "count", 42,
//     "user", User{Name: "alice"},
//  )
// is the equivalent of
//   unsugared.With(
//     String("hello", "world"),
//     String("failure", "oh no"),
//     Stack(),
//     Int("count", 42),
//     Object("user", User{Name: "alice"}),
//   )
//
// Note that the keys in key-value pairs should be strings. In development,
// passing a non-string key panics. In production, the logger is more
// forgiving: a separate error is logged, but the key-value pair is skipped
// and execution continues. Passing an orphaned key triggers similar behavior:
// panics in development and errors in production.

// func createOrder(Code string, Name string, UnitPrice int64, Quantity int32) {
// 	var order pb.Order

// 	aggregateID := uuid.NewV4().String()
// 	order.OrderId = aggregateID
// 	order.Text = "Pending"
// 	order.Time = time.Now().Unix()
// 	order.Tablename = "haha"
// 	//设置服务名
// 	//设置时间
// 	//设置日志内容

// 	// //调用发送消息
// 	err := createOrderRPC(order)
// 	log.Print("1")
// 	if err != nil {
// 		fmt.Println("grpc错误")
// 		log.Print(err)
// 		return
// 	}
// }
func createOrder(Tablename string, Time int64, Text string) {
	var order pb.Order

	aggregateID := uuid.NewV4().String()
	order.OrderId = aggregateID
	order.Tablename = Tablename
	order.Time = Time
	order.Text = Text
	//设置服务名
	//设置时间
	//设置日志内容
	// order.OrderItems[0].Code = Code
	// order.OrderItems[0].Name = Name
	// order.OrderItems[0].UnitPrice = UnitPrice
	// order.OrderItems[0].Quantity = Quantity

	//调用发送消息
	err := createOrderRPC(order)
	if err != nil {
		fmt.Println("grpc错误")
		log.Print(err)
		return
	}
}

//发送grpc消息
func createOrderRPC(order pb.Order) error {
	//连接grpc服务器
	conn, err := grpc.Dial(grpcUri, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Unable to connect: %v", err)
	}

	defer conn.Close()
	client := pb.NewEventStoreClient(conn)
	orderJSON, _ := json.Marshal(order)

	event := &pb.Event{
		EventId:       uuid.NewV4().String(),
		EventType:     event,
		AggregateId:   order.OrderId,
		AggregateType: aggregate,
		EventData:     string(orderJSON),
		Channel:       channel,
	}

	resp, err := client.CreateEvent(context.Background(), event)
	if err != nil {
		return errors.Wrap(err, "Error from RPC server")
	}
	if resp.IsSuccess {
		return nil
	} else {
		return errors.Wrap(err, "Error from RPC server")
	}

}

func (s *SugaredLogger) With(args ...interface{}) *SugaredLogger {
	return &SugaredLogger{base: s.base.With(s.sweetenFields(args)...)}
}

// Debug uses fmt.Sprint to construct and log a message.
func (s *SugaredLogger) Debug(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息

	createOrder(SerName, t1, c)
	s.log(DebugLevel, "", args, nil)
}

// Info uses fmt.Sprint to construct and log a message.//监听grpc,将信息插入到数据库
func (s *SugaredLogger) Info(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}

	t1 := time.Now().Unix() //1564552562
	//发送订单信息

	createOrder(SerName, t1, c)
	s.log(InfoLevel, "", args, nil)

}

// Warn uses fmt.Sprint to construct and log a message.
func (s *SugaredLogger) Warn(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(WarnLevel, "", args, nil)
}

// Error uses fmt.Sprint to construct and log a message.
func (s *SugaredLogger) Error(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(ErrorLevel, "", args, nil)
}

// DPanic uses fmt.Sprint to construct and log a message. In development, the
// logger then panics. (See DPanicLevel for details.)
func (s *SugaredLogger) DPanic(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(DPanicLevel, "", args, nil)
}

// Panic uses fmt.Sprint to construct and log a message, then panics.
func (s *SugaredLogger) Panic(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(PanicLevel, "", args, nil)
}

// Fatal uses fmt.Sprint to construct and log a message, then calls os.Exit.
func (s *SugaredLogger) Fatal(args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(FatalLevel, "", args, nil)
}

// Debugf uses fmt.Sprintf to log a templated message.
func (s *SugaredLogger) Debugf(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(DebugLevel, template, args, nil)
}

// Infof uses fmt.Sprintf to log a templated message.
func (s *SugaredLogger) Infof(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(InfoLevel, template, args, nil)
}

// Warnf uses fmt.Sprintf to log a templated message.
func (s *SugaredLogger) Warnf(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(WarnLevel, template, args, nil)
}

// Errorf uses fmt.Sprintf to log a templated message.
func (s *SugaredLogger) Errorf(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(ErrorLevel, template, args, nil)
}

// DPanicf uses fmt.Sprintf to log a templated message. In development, the
// logger then panics. (See DPanicLevel for details.)
func (s *SugaredLogger) DPanicf(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(DPanicLevel, template, args, nil)
}

// Panicf uses fmt.Sprintf to log a templated message, then panics.
func (s *SugaredLogger) Panicf(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(PanicLevel, template, args, nil)
}

// Fatalf uses fmt.Sprintf to log a templated message, then calls os.Exit.
func (s *SugaredLogger) Fatalf(template string, args ...interface{}) {
	//循环遍历不定参数
	var c string
	for _, item := range args {

		if value, ok := item.(error); ok {
			//知识err类型
			c = c + value.Error()
		} else if value, ok := item.(string); ok {
			c = c + value
		} else if value, ok := item.(int); ok {
			//将int类型转换为string类型
			str1 := strconv.Itoa(value)
			c = c + str1
		}

	}
	fmt.Println(c)
	t1 := time.Now().Unix() //1564552562
	//发送订单信息
	createOrder(SerName, t1, c)
	s.log(FatalLevel, template, args, nil)
}

// Debugw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
//
// When debug-level logging is disabled, this is much faster than
//  s.With(keysAndValues).Debug(msg)
func (s *SugaredLogger) Debugw(msg string, keysAndValues ...interface{}) {
	s.log(DebugLevel, msg, nil, keysAndValues)
}

// Infow logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func (s *SugaredLogger) Infow(msg string, keysAndValues ...interface{}) {
	s.log(InfoLevel, msg, nil, keysAndValues)
}

// Warnw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func (s *SugaredLogger) Warnw(msg string, keysAndValues ...interface{}) {
	s.log(WarnLevel, msg, nil, keysAndValues)
}

// Errorw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func (s *SugaredLogger) Errorw(msg string, keysAndValues ...interface{}) {
	s.log(ErrorLevel, msg, nil, keysAndValues)
}

// DPanicw logs a message with some additional context. In development, the
// logger then panics. (See DPanicLevel for details.) The variadic key-value
// pairs are treated as they are in With.
func (s *SugaredLogger) DPanicw(msg string, keysAndValues ...interface{}) {
	s.log(DPanicLevel, msg, nil, keysAndValues)
}

// Panicw logs a message with some additional context, then panics. The
// variadic key-value pairs are treated as they are in With.
func (s *SugaredLogger) Panicw(msg string, keysAndValues ...interface{}) {
	s.log(PanicLevel, msg, nil, keysAndValues)
}

// Fatalw logs a message with some additional context, then calls os.Exit. The
// variadic key-value pairs are treated as they are in With.
func (s *SugaredLogger) Fatalw(msg string, keysAndValues ...interface{}) {
	s.log(FatalLevel, msg, nil, keysAndValues)
}

// Sync flushes any buffered log entries.
func (s *SugaredLogger) Sync() error {
	return s.base.Sync()
}

func (s *SugaredLogger) log(lvl zapcore.Level, template string, fmtArgs []interface{}, context []interface{}) {
	// If logging at this level is completely disabled, skip the overhead of
	// string formatting.
	if lvl < DPanicLevel && !s.base.Core().Enabled(lvl) {
		return
	}

	// Format with Sprint, Sprintf, or neither.
	msg := template
	if msg == "" && len(fmtArgs) > 0 {
		msg = fmt.Sprint(fmtArgs...)
	} else if msg != "" && len(fmtArgs) > 0 {
		msg = fmt.Sprintf(template, fmtArgs...)
	}

	if ce := s.base.Check(lvl, msg); ce != nil {
		ce.Write(s.sweetenFields(context)...)
	}
}

func (s *SugaredLogger) sweetenFields(args []interface{}) []Field {
	if len(args) == 0 {
		return nil
	}

	// Allocate enough space for the worst case; if users pass only structured
	// fields, we shouldn't penalize them with extra allocations.
	fields := make([]Field, 0, len(args))
	var invalid invalidPairs

	for i := 0; i < len(args); {
		// This is a strongly-typed field. Consume it and move on.
		if f, ok := args[i].(Field); ok {
			fields = append(fields, f)
			i++
			continue
		}

		// Make sure this element isn't a dangling key.
		if i == len(args)-1 {
			s.base.DPanic(_oddNumberErrMsg, Any("ignored", args[i]))
			break
		}

		// Consume this value and the next, treating them as a key-value pair. If the
		// key isn't a string, add this pair to the slice of invalid pairs.
		key, val := args[i], args[i+1]
		if keyStr, ok := key.(string); !ok {
			// Subsequent errors are likely, so allocate once up front.
			if cap(invalid) == 0 {
				invalid = make(invalidPairs, 0, len(args)/2)
			}
			invalid = append(invalid, invalidPair{i, key, val})
		} else {
			fields = append(fields, Any(keyStr, val))
		}
		i += 2
	}

	// If we encountered any invalid key-value pairs, log an error.
	if len(invalid) > 0 {
		s.base.DPanic(_nonStringKeyErrMsg, Array("invalid", invalid))
	}
	return fields
}

type invalidPair struct {
	position   int
	key, value interface{}
}

func (p invalidPair) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt64("position", int64(p.position))
	Any("key", p.key).AddTo(enc)
	Any("value", p.value).AddTo(enc)
	return nil
}

type invalidPairs []invalidPair

func (ps invalidPairs) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	var err error
	for i := range ps {
		err = multierr.Append(err, enc.AppendObject(ps[i]))
	}
	return err
}
