package interceptor

import (
	"context"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"sr.ht/moyanhao/bedrock-metaserver/errors"
)

// UnaryErrorInterceptor is a gRPC unary interceptor that handles custom errors and converts them to gRPC status codes
func UnaryErrorInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Call the actual handler function
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}

		// Handle custom error types
		var customErr *errors.Error
		if errors.As(err, &customErr) {
			// Log error details including stack trace
			log.Errorf("RPC error - Method: %s, Code: %s, Message: %s, Stack: %s",
				info.FullMethod,
				customErr.Code,
				customErr.Message,
				customErr.Stack,
			)

			// Map to gRPC status code based on error code
			grpcCode := mapToGrpcCode(customErr.Code)
			return nil, status.Errorf(grpcCode, "%s (code: %s)", customErr.Message, customErr.Code)
		}

		// Handle other types of errors
		log.Errorf("RPC error - Method: %s, Error: %v", info.FullMethod, err)
		return nil, status.Errorf(codes.Internal, "internal server error: %v", err)
	}
}

// StreamErrorInterceptor is a gRPC stream interceptor that handles custom errors and converts them to gRPC status codes
func StreamErrorInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		stream grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// Create a wrapped stream to capture and handle errors
		wrappedStream := &wrappedServerStream{
			ServerStream: stream,
			ctx:          stream.Context(),
		}

		// Call the actual handler function
		err := handler(srv, wrappedStream)
		if err == nil {
			return nil
		}

		// Handle custom error types
		var customErr *errors.Error
		if errors.As(err, &customErr) {
			// Log error details including stack trace
			log.Errorf("Stream RPC error - Method: %s, Code: %s, Message: %s, Stack: %s",
				info.FullMethod,
				customErr.Code,
				customErr.Message,
				customErr.Stack,
			)

			// Map to gRPC status code based on error code
			grpcCode := mapToGrpcCode(customErr.Code)
			return status.Errorf(grpcCode, "%s (code: %s)", customErr.Message, customErr.Code)
		}

		// Handle other types of errors
		log.Errorf("Stream RPC error - Method: %s, Error: %v", info.FullMethod, err)
		return status.Errorf(codes.Internal, "internal server error: %v", err)
	}
}

// wrappedServerStream is a wrapper for grpc.ServerStream
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapped context
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}

// mapToGrpcCode maps custom error codes to gRPC status codes
func mapToGrpcCode(errorCode string) codes.Code {
	switch errorCode {
	case errors.ErrCodeInvalidArgument:
		return codes.InvalidArgument
	case errors.ErrCodeNotFound:
		return codes.NotFound
	case errors.ErrCodeAlreadyExists:
		return codes.AlreadyExists
	case errors.ErrCodePermissionDenied:
		return codes.PermissionDenied
	case errors.ErrCodeResourceExhausted:
		return codes.ResourceExhausted
	case errors.ErrCodeFailedPrecondition:
		return codes.FailedPrecondition
	case errors.ErrCodeAborted:
		return codes.Aborted
	case errors.ErrCodeUnavailable:
		return codes.Unavailable
	case errors.ErrCodeUnauthenticated:
		return codes.Unauthenticated
	case errors.ErrCodeNotLeader:
		return codes.FailedPrecondition
	case errors.ErrCodeNotSupported:
		return codes.Unimplemented
	case errors.ErrCodeTimeout:
		return codes.DeadlineExceeded
	default:
		return codes.Internal
	}
}

// Provide a default interceptor for backward compatibility
func DefaultErrorInterceptor() grpc.UnaryServerInterceptor {
	return UnaryErrorInterceptor()
}
