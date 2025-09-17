"""
gRPC Demo Server - Team Presentation
=====================================

This example demonstrates:
1. Protocol Buffers for service definition
2. gRPC server-side streaming
3. Performance comparison between gRPC and REST
4. Real-world data handling scenarios

Prerequisites:
- pip install grpcio grpcio-tools
- Generated protobuf files (data_service_pb2.py, data_service_pb2_grpc.py)
"""

import grpc
from concurrent import futures
import time
import random
import threading
from datetime import datetime

# Generated protobuf classes
import data_service_pb2
import data_service_pb2_grpc


class EmployeeDataService(data_service_pb2_grpc.SimpleDataServiceServicer):
    """
    gRPC Service Implementation
    
    Demonstrates server-side streaming for efficient data transfer
    """
    
    def __init__(self):
        self.employees = self._generate_sample_data()
        print(f"✅ Initialized with {len(self.employees)} employee records")
    
    def _generate_sample_data(self):
        """Generate realistic employee data for demonstration"""
        departments = ["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations"]
        positions = ["Manager", "Senior", "Junior", "Lead", "Director", "Analyst"]
        
        employees = []
        for i in range(1, 10001):  # 10K employees
            dept = random.choice(departments)
            position = random.choice(positions)
            salary = random.randint(45000, 150000)
            
            # Create realistic employee data
            employee_data = (
                f"ID: {i:05d} | "
                f"Name: {position} {dept} Employee {i} | "
                f"Email: employee{i}@company.com | "
                f"Department: {dept} | "
                f"Salary: ${salary:,} | "
                f"Hire Date: 2020-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
            )
            employees.append(employee_data)
        
        return employees
    
    def StreamLargeData(self, request, context):
        """
        Server-side streaming RPC
        
        Streams employee data efficiently using gRPC's built-in streaming capabilities.
        This demonstrates how gRPC handles large datasets better than traditional REST APIs.
        """
        start_time = time.time()
        
        print(f"\n🚀 Starting gRPC stream at {datetime.now().strftime('%H:%M:%S')}")
        print(f"📊 Streaming {len(self.employees)} employee records...")
        
        try:
            for i, employee_data in enumerate(self.employees):
                # Stream each employee record
                yield data_service_pb2.LargeDataLine(line=employee_data)
                
                # Progress indicator for demo purposes
                if (i + 1) % 2500 == 0:
                    elapsed = time.time() - start_time
                    print(f"   📈 Streamed {i + 1:,} records in {elapsed:.2f}s")
            
            total_time = time.time() - start_time
            print(f"✅ Completed streaming {len(self.employees):,} records in {total_time:.2f}s")
            print(f"📊 Throughput: {len(self.employees)/total_time:.0f} records/second\n")
            
        except Exception as e:
            print(f"❌ Error during streaming: {e}")
            context.set_details(f'Streaming error: {str(e)}')
            context.set_code(grpc.StatusCode.INTERNAL)


def run_grpc_server():
    """
    Start the gRPC server
    
    Configures and runs the gRPC server with proper thread pool management
    """
    # Create server with thread pool
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.keepalive_time_ms', 30000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True)
        ]
    )
    
    # Add our service to the server
    data_service_pb2_grpc.add_SimpleDataServiceServicer_to_server(
        EmployeeDataService(), server
    )
    
    # Listen on all interfaces
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    
    print("=" * 70)
    print("🚀 gRPC Employee Data Service - TEAM DEMO")
    print("=" * 70)
    print(f"📡 Server listening on: localhost:50051")
    print(f"📋 Service: SimpleDataService")
    print(f"🔄 Method: StreamLargeData (Server-side streaming)")
    print(f"📊 Dataset: 10,000 employee records")
    print("=" * 70)
    
    server.start()
    
    try:
        print(f"⏰ Server started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("🔥 Ready for gRPC-Web connections!")
        print("👀 Watching for incoming requests...\n")
        
        # Keep server running
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Shutting down gRPC server...")
        server.stop(grace=5)
        print("✅ Server stopped gracefully")


if __name__ == '__main__':
    """
    Entry point for the gRPC demo server
    
    Usage:
    1. Start this server: python server.py
    2. Start grpcwebproxy: grpcwebproxy --backend_addr=localhost:50051 --run_tls_server=false --allow_all_origins
    3. Open the HTML demo page to see gRPC vs REST comparison
    """
    run_grpc_server()