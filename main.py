import asyncio
import aiohttp
import time
import argparse
import statistics
from dataclasses import dataclass
from typing import List, Dict
import logging
from concurrent.futures import ThreadPoolExecutor
import struct

@dataclass
class StreamMetrics:
    total_bytes: int = 0
    packet_count: int = 0
    start_time: float = 0
    last_packet_time: float = 0
    bitrates: List[float] = None
    latencies: List[float] = None

    def __post_init__(self):
        self.bitrates = []
        self.latencies = []

class FLVLoadTester:
    def __init__(self, url: str, num_clients: int, duration: int):
        self.url = url
        self.num_clients = num_clients
        self.duration = duration
        self.client_metrics: Dict[int, StreamMetrics] = {}
        self.running = True
        
        # Setup logging
        logging.basicConfig(
            level=logging.DEBUG,  # Changed to DEBUG level for more detailed logs
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    async def parse_flv_header(self, data: bytes) -> bool:
        """Parse FLV header to verify the stream."""
        try:
            self.logger.debug(f"Received header data (hex): {data.hex()}")
            if len(data) < 9:
                self.logger.error(f"Header too short: {len(data)} bytes")
                return False
            
            # Check FLV signature
            if data[:3] != b'FLV':
                self.logger.error(f"Invalid FLV signature: {data[:3]}")
                return False
                
            version = data[3]
            flags = data[4]
            header_size = struct.unpack('>I', data[5:9])[0]
            
            self.logger.info(f"FLV Header: version={version}, flags={flags}, header_size={header_size}")
            return True
        except Exception as e:
            self.logger.error(f"Error parsing FLV header: {e}")
            return False

    async def process_flv_packets(self, data: bytes, client_id: int):
        """Process FLV packets and update metrics."""
        metrics = self.client_metrics[client_id]
        current_time = time.time()
        
        # Update metrics
        metrics.total_bytes += len(data)
        metrics.packet_count += 1
        
        # Calculate latency
        if metrics.last_packet_time > 0:
            latency = current_time - metrics.last_packet_time
            metrics.latencies.append(latency)
        
        # Calculate bitrate (bits per second)
        if metrics.last_packet_time > 0:
            time_diff = current_time - metrics.last_packet_time
            if time_diff > 0:
                bitrate = (len(data) * 8) / time_diff
                metrics.bitrates.append(bitrate)
        
        metrics.last_packet_time = current_time
        
        # Log packet details periodically
        if metrics.packet_count % 100 == 0:
            self.logger.debug(f"Client {client_id}: Received packet {metrics.packet_count}, "
                            f"size={len(data)} bytes, total={metrics.total_bytes} bytes")

    async def client_session(self, client_id: int):
        """Simulate a single client session."""
        self.client_metrics[client_id] = StreamMetrics(start_time=time.time())
        
        try:
            timeout = aiohttp.ClientTimeout(total=None, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                self.logger.info(f"Client {client_id}: Connecting to {self.url}")
                
                headers = {
                    'User-Agent': 'FLVLoadTester/1.0',
                    'Connection': 'keep-alive',
                    'Accept': '*/*'
                }
                
                async with session.get(self.url, headers=headers) as response:
                    self.logger.info(f"Client {client_id}: Connected with status {response.status}")
                    self.logger.debug(f"Client {client_id}: Response headers: {response.headers}")
                    
                    if response.status != 200:
                        self.logger.error(f"Client {client_id}: HTTP {response.status}")
                        return

                    # Initialize stream reading
                    content_type = response.headers.get('Content-Type', '')
                    self.logger.info(f"Client {client_id}: Content-Type: {content_type}")

                    # Read stream data without trying to parse header
                    while self.running:
                        try:
                            chunk = await response.content.read(8192)
                            if not chunk:
                                self.logger.warning(f"Client {client_id}: End of stream reached")
                                break
                            
                            await self.process_flv_packets(chunk, client_id)
                            
                        except asyncio.CancelledError:
                            self.logger.info(f"Client {client_id}: Session cancelled")
                            break
                        except Exception as e:
                            self.logger.error(f"Client {client_id}: Error reading stream: {e}")
                            break

        except asyncio.CancelledError:
            self.logger.info(f"Client {client_id}: Session cancelled")
        except Exception as e:
            self.logger.error(f"Client {client_id}: Connection error: {e}")
        finally:
            self.logger.info(f"Client {client_id}: Session ended")

    def print_statistics(self):
        """Print test statistics."""
        total_bytes = 0
        total_packets = 0
        all_bitrates = []
        all_latencies = []
        
        for client_id, metrics in self.client_metrics.items():
            duration = metrics.last_packet_time - metrics.start_time if metrics.last_packet_time > 0 else 0
            mbps = (metrics.total_bytes * 8) / (1024 * 1024 * duration) if duration > 0 else 0
            
            self.logger.info(f"\nClient {client_id} Statistics:")
            self.logger.info(f"Total data received: {metrics.total_bytes / (1024*1024):.2f} MB")
            self.logger.info(f"Average bitrate: {mbps:.2f} Mbps")
            self.logger.info(f"Packets received: {metrics.packet_count}")
            self.logger.info(f"Duration: {duration:.2f} seconds")
            
            if metrics.latencies:
                self.logger.info(f"Average latency: {statistics.mean(metrics.latencies)*1000:.2f} ms")
                self.logger.info(f"Max latency: {max(metrics.latencies)*1000:.2f} ms")
            
            total_bytes += metrics.total_bytes
            total_packets += metrics.packet_count
            all_bitrates.extend(metrics.bitrates)
            all_latencies.extend(metrics.latencies)
        
        self.logger.info("\nOverall Statistics:")
        self.logger.info(f"Total clients: {self.num_clients}")
        self.logger.info(f"Total data received: {total_bytes / (1024*1024):.2f} MB")
        self.logger.info(f"Total packets received: {total_packets}")
        
        if all_bitrates:
            self.logger.info(f"Average bitrate across all clients: {statistics.mean(all_bitrates)/1024/1024:.2f} Mbps")
        if all_latencies:
            self.logger.info(f"Average latency across all clients: {statistics.mean(all_latencies)*1000:.2f} ms")

    async def run(self):
        """Run the load test."""
        self.logger.info(f"Starting load test with {self.num_clients} clients for {self.duration} seconds")
        
        try:
            # Start client sessions
            tasks = [asyncio.create_task(self.client_session(i)) for i in range(self.num_clients)]
            
            # Run for specified duration
            await asyncio.sleep(self.duration)
            self.running = False
            
            # Wait for all clients to finish
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"Error in main run loop: {e}")
        finally:
            # Print final statistics
            self.print_statistics()

def main():
    parser = argparse.ArgumentParser(description='HTTP-FLV Load Tester')
    parser.add_argument('url', help='URL of the HTTP-FLV stream')
    parser.add_argument('--clients', type=int, default=10, help='Number of clients to simulate')
    parser.add_argument('--duration', type=int, default=60, help='Test duration in seconds')
    args = parser.parse_args()

    tester = FLVLoadTester(args.url, args.clients, args.duration)
    asyncio.run(tester.run())

if __name__ == '__main__':
    main()