import asyncio
import json
import logging
import aio_pika
from core.config import settings
from services.vector_store import vector_store_service

async def start_rabbitmq_consumer():
    """Connect to RabbitMQ and consume RAG sync messages, routing each to the correct company collection."""
    retry_delay = 5
    while True:
        try:
            connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
            async with connection:
                channel = await connection.channel()
                await channel.set_qos(prefetch_count=5)

                queue = await channel.declare_queue("rag.sync.queue", durable=True)
                logging.info("RabbitMQ consumer connected, waiting for messages...")

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        async with message.process():
                            try:
                                body = json.loads(message.body.decode())
                                content = body.get("content", "")
                                feature = body.get("feature")
                                company_name = body.get("companyName")
                                if not company_name:
                                    logging.error("Received RAG sync message without companyName. Skipping.")
                                    continue
                                if content:
                                    await asyncio.get_event_loop().run_in_executor(
                                        None,
                                        lambda: vector_store_service.ingest_text(
                                            content,
                                            {"feature": feature} if feature else {},
                                            company_name,
                                        )
                                    )
                                    logging.info(f"Ingested message: feature={feature}, company={company_name}")
                            except Exception as e:
                                logging.error(f"Failed to process RAG sync message: {e}")
        except Exception as e:
            logging.warning(f"RabbitMQ connection failed: {e}. Retrying in {retry_delay}s...")
            await asyncio.sleep(retry_delay)
