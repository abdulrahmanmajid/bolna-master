import os
import json
import requests
import uuid
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.rest import Client
from dotenv import load_dotenv
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, Request, Path, Depends, APIRouter
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import PlainTextResponse, JSONResponse
from logging import Logger
from pydantic import BaseModel
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import asyncpg
import logging
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from datetime import datetime
import pytz
from typing import Optional, Dict, List, Any
from contextlib import asynccontextmanager
import stripe
import aiohttp
import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)
redis_pool = redis.ConnectionPool.from_url(os.getenv('REDIS_URL'), decode_responses=True)
redis_client = redis.Redis.from_pool(redis_pool)
# Initialize scheduler
scheduler = AsyncIOScheduler()
scheduler.start()
app = FastAPI()
load_dotenv()
port = 8001

stripe.api_key = os.getenv('STRIPE_SECRET_KEY')

twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
twilio_phone_number = os.getenv('TWILIO_PHONE_NUMBER')

# Initialize Twilio client
twilio_client = Client(twilio_account_sid, twilio_auth_token)

# Global pool variable
_pool: Optional[asyncpg.Pool] = None

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION'),
    config=Config(signature_version='s3v4')
)

async def init_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.getenv('DATABASE_URL'),
            min_size=2,
            max_size=50,
            max_inactive_connection_lifetime=300.0,
            command_timeout=60
        )
    return _pool

async def get_db_pool():
    """Get the database pool, initializing it if necessary"""
    global _pool
    if _pool is None:
        await init_db_pool()
    return _pool

@asynccontextmanager
async def get_db_conn():
    """Context manager for database connections"""
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        try:
            yield conn
        except Exception as e:
            logger.error(f"Database error: {str(e)}")
            raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db_pool()
    
    yield  # Server is running and handling requests
    
    # Shutdown
    global _pool
    if (_pool):
        await _pool.close()

# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

def populate_custom_urls():
    """Returns custom domain URLs for HTTP and WebSocket connections"""
    app_callback_url = "https://api.kallabot.com"
    websocket_url = "wss://ws.kallabot.com"
    return app_callback_url, websocket_url

security = HTTPBearer()

async def get_account_from_api_key(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> uuid.UUID:
    try:
        token = credentials.credentials
        if not token.startswith("sk_"):
            raise HTTPException(
                status_code=401,
                detail="Invalid API key format"
            )
        
        async with get_db_conn() as conn:
            account = await conn.fetchrow(
                'SELECT account_id FROM accounts WHERE api_key = $1',
                token
            )
            
            if not account:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid API key"
                )
            
            return account['account_id']
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail="Invalid authentication credentials"
        )





##############################################
# Outbound Calls
#############################################

@app.post('/call')
async def make_call(
    request: Request,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        call_details = await request.json()
        agent_id = call_details.get('agent_id', None)

        if not agent_id:
            raise HTTPException(status_code=404, detail="Agent ID not provided")

        async with get_db_conn() as conn:
            # Get all necessary database info in one connection context
            agent = await conn.fetchrow(
                'SELECT call_direction, is_compliant FROM agents WHERE agent_id = $1',
                uuid.UUID(agent_id)
            )
            
            if not agent:
                raise HTTPException(status_code=404, detail="Agent not found")
                
            if agent['call_direction'] == 'inbound':
                raise HTTPException(
                    status_code=403, 
                    detail="This agent is configured for inbound calls only"
                )

            # Get subaccount info
            subaccount_info = await conn.fetchrow(
                'SELECT twilio_subaccount_sid, twilio_subaccount_auth_token FROM accounts WHERE account_id = $1',
                account_id
            )
            
            if not subaccount_info:
                raise HTTPException(status_code=404, detail="Account not found")

            sender_phone_number = call_details.get('sender_phone_number', None)
            recipient_phone_number = call_details.get('recipient_phone_number', None)
            record = call_details.get('record', False)
            template_variables = call_details.get('template_variables', {})

            # Debug logging
            logger.info(f"Received template variables: {template_variables}")
            
            try:
                # Validate JSON serialization
                json_str = json.dumps(template_variables)
                redis_key = f"template_variables:{agent_id}"
                await redis_client.set(redis_key, json_str, ex=3600)
            except json.JSONDecodeError as je:
                logger.error(f"JSON serialization error: {str(je)}")
                raise HTTPException(status_code=400, detail=f"Invalid template variables format: {str(je)}")
            except Exception as e:
                logger.error(f"Redis storage error: {str(e)}")
                raise HTTPException(status_code=500, detail="Failed to store template variables")

            if not agent_id or not account_id:
                raise HTTPException(status_code=404, detail="Agent ID or Account ID not provided")
            
            if not call_details or "recipient_phone_number" not in call_details:
                raise HTTPException(status_code=404, detail="Recipient phone number not provided")

            # Create Twilio client for subaccount
            subaccount_client = Client(twilio_account_sid, twilio_auth_token, subaccount_info['twilio_subaccount_sid'])

            app_callback_url, websocket_url = populate_custom_urls()

            # Configure recording callback if recording is enabled
            recording_status_callback = None
            if record:
                recording_status_callback = f"{app_callback_url}/recording_callback"

            try:
                call = subaccount_client.calls.create(
                    to=recipient_phone_number,
                    from_=sender_phone_number,
                    url=f"{app_callback_url}/twilio_callback?ws_url={websocket_url}&agent_id={agent_id}&account_id={account_id}",
                    method="POST",
                    record=not agent['is_compliant'],  # Record only if agent is not compliant
                    recording_status_callback=recording_status_callback if not agent['is_compliant'] else None,
                    recording_status_callback_event=['completed'] if not agent['is_compliant'] else None,
                    status_callback=f"{app_callback_url}/call_status_callback",
                    status_callback_event=['completed', 'no-answer', 'busy', 'failed'],
                    status_callback_method='POST',
                    timeout=30
                )
                
                # Record the call in the database
                async with conn.transaction():
                    await conn.execute(
                        'INSERT INTO calls (call_sid, agent_id, account_id, from_number, to_number) VALUES ($1, $2, $3, $4, $5)',
                        call.sid, uuid.UUID(agent_id), account_id, sender_phone_number, recipient_phone_number
                    )
                    await conn.execute(
                        'UPDATE agents SET total_calls = total_calls + 1 WHERE agent_id = $1',
                        uuid.UUID(agent_id)
                    )
                    await conn.execute(
                        'UPDATE accounts SET total_calls = total_calls + 1 WHERE account_id = $1',
                        account_id
                    )
                    
                    # Get agent details for response
                    agent = await conn.fetchrow(
                        'SELECT name, agent_type FROM agents WHERE agent_id = $1',
                        uuid.UUID(agent_id)
                    )

                return JSONResponse({
                    "status": "success",
                    "message": "Call initiated successfully",
                    "call_details": {
                        "call_sid": call.sid,
                        "from_number": sender_phone_number,
                        "to_number": recipient_phone_number,
                        "recording_enabled": record,
                        "created_at": datetime.now().isoformat(),
                    },
                }, status_code=200)

            except Exception as e:
                print(e)
                return JSONResponse({"error": str(e)}, status_code=500)

    except HTTPException as he:
        # Properly propagate HTTP exceptions with their status codes
        return JSONResponse(
            {"error": he.detail},
            status_code=he.status_code
        )
    except Exception as e:
        print(f"Exception occurred in make_call: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
    

# End Call

class EndCallPayload(BaseModel):
    call_sid: str

@app.post('/call/end')
async def end_call(
    payload: EndCallPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        # Get subaccount credentials
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            subaccount_info = await conn.fetchrow(
                'SELECT twilio_subaccount_sid, twilio_subaccount_auth_token FROM accounts WHERE account_id = $1',
                account_id
            )
        
        if not subaccount_info:
            raise HTTPException(status_code=404, detail="Account not found")

        # Create Twilio client for subaccount
        subaccount_client = Client(
            twilio_account_sid, 
            twilio_auth_token, 
            subaccount_info['twilio_subaccount_sid']
        )

        # End the call by updating its status to 'completed'
        try:
            call = subaccount_client.calls(payload.call_sid).update(status='completed')
            return {
                "status": "success",
                "message": "Call ended successfully",
                "call_sid": payload.call_sid
            }
        except TwilioRestException as e:
            logger.error(f"Twilio error ending call: {str(e)}")
            raise HTTPException(status_code=400, detail="Failed to end call: Invalid call SID or call already ended")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error ending call: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")



# Twilio Callback

@app.post('/twilio_callback')
async def twilio_callback(ws_url: str = Query(...), agent_id: str = Query(...), account_id: str = Query(...)):
    try:
        response = VoiceResponse()

        connect = Connect()
        websocket_twilio_route = f'{ws_url}/v1/chat/{agent_id}'
        connect.stream(url=websocket_twilio_route)
        print(f"websocket connection done to {websocket_twilio_route}")
        response.append(connect)

        return PlainTextResponse(str(response), status_code=200, media_type='text/xml')

    except Exception as e:
        print(f"Exception occurred in twilio_callback: {e}")

@app.post('/call_status_callback')
async def call_status_callback(request: Request):
    try:
        form_data = await request.form()
        call_sid = form_data.get('CallSid')
        call_status = form_data.get('CallStatus')
        call_duration = form_data.get('CallDuration')  # Duration in seconds
        
        if call_status not in ['completed', 'failed']:
            return PlainTextResponse("OK")
            
        await asyncio.sleep(5)
        
        # Get transcription from Redis
        redis_key = f"transcription:{call_sid}"
        transcription_json_str = await redis_client.get(redis_key)
        
        async with get_db_conn() as conn:
            async with conn.transaction():
                # Get the call details to update account and agent stats
                call_details = await conn.fetchrow('''
                    SELECT agent_id, account_id 
                    FROM calls 
                    WHERE call_sid = $1
                ''', call_sid)
                
                if call_details:
                    # Calculate call cost using our new pricing function
                    call_cost = 0
                    if call_duration and call_status == 'completed':
                        call_cost = await calculate_call_cost(
                            int(call_duration), 
                            call_details['account_id']
                        )

                    # Update call record with duration, status, call_type and cost
                    await conn.execute('''
                        UPDATE calls 
                        SET status = $1,
                            duration = $2,
                            transcription = $3::jsonb,
                            call_type = 'outbound',
                            cost = $4
                        WHERE call_sid = $5
                    ''', call_status, float(call_duration) if call_duration else 0, 
                        transcription_json_str if transcription_json_str else None,
                        call_cost,
                        call_sid)
                    
                    # Update agent's total duration and cost
                    await conn.execute('''
                        UPDATE agents 
                        SET total_duration = total_duration + $1,
                            total_cost = total_cost + $2
                        WHERE agent_id = $3
                    ''', float(call_duration) if call_duration else 0, 
                        call_cost,
                        call_details['agent_id'])
                    
                    # Update account's total duration and cost
                    await conn.execute('''
                        UPDATE accounts 
                        SET total_duration = total_duration + $1,
                            total_cost = total_cost + $2
                        WHERE account_id = $3
                    ''', float(call_duration) if call_duration else 0, 
                        call_cost,
                        call_details['account_id'])
                
                # Delete from Redis after successful DB update
                if transcription_json_str:
                    await redis_client.delete(redis_key)
                
        return PlainTextResponse("OK")
    except Exception as e:
        logger.error(f"Error in call_status_callback: {str(e)}")
        return PlainTextResponse("Error", status_code=500)

@app.post('/recording_callback')
async def recording_callback(request: Request):
    try:
        form_data = await request.form()
        recording_url = form_data.get('RecordingUrl')
        call_sid = form_data.get('CallSid')
        recording_status = form_data.get('RecordingStatus')
        
        if recording_status != 'completed':
            return PlainTextResponse("OK")
            
        # Wait for recording to be fully processed
        await asyncio.sleep(7)
        
        # Download recording from Twilio
        auth = aiohttp.BasicAuth(twilio_account_sid, twilio_auth_token)
        async with aiohttp.ClientSession(auth=auth) as session:
            async with session.get(f"{recording_url}.wav") as response:
                if response.status == 200:
                    # Read the audio data
                    audio_data = await response.read()
                    
                    # Generate S3 key with timestamp for uniqueness
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    # Note: Using the existing folder structure in your bucket
                    s3_key = f"recordingskallabot/recordings_{call_sid}_{timestamp}.wav"
                    
                    try:
                        # Upload to S3 using the correct bucket name
                        s3_client.put_object(
                            Bucket='kallabotbucket',  # Using your actual bucket name
                            Key=s3_key,
                            Body=audio_data,
                            ContentType='audio/wav'
                        )
                        
                        # Generate S3 URL using the correct structure
                        s3_url = f"https://kallabotbucket.s3.us-east-1.amazonaws.com/{s3_key}"
                        
                        # Update database with S3 URL
                        async with get_db_conn() as conn:
                            await conn.execute('''
                                UPDATE calls 
                                SET recording_url = $1
                                WHERE call_sid = $2
                            ''', s3_url, call_sid)
                            
                        logger.info(f"Successfully uploaded recording to S3: {s3_url}")
                        
                        # Clean up Twilio recording
                        try:
                            delete_url = recording_url.replace('.json', '')
                            async with session.delete(delete_url) as delete_response:
                                if delete_response.status not in [200, 204]:
                                    logger.error(f"Failed to delete Twilio recording: {delete_response.status}")
                        except Exception as e:
                            logger.error(f"Error deleting Twilio recording: {str(e)}")
                            
                    except Exception as s3_error:
                        logger.error(f"S3 upload error: {str(s3_error)}")
                        return PlainTextResponse("Error uploading to S3", status_code=500)
                else:
                    logger.error(f"Failed to download recording: {response.status}")
                    return PlainTextResponse("Error downloading recording", status_code=500)
                
        return PlainTextResponse("OK")
    except Exception as e:
        logger.error(f"Error in recording_callback: {str(e)}")
        return PlainTextResponse("Error", status_code=500)


##############################################
# Campaign Outbound Calls
#############################################

# Create Campaign Model

class CreateCampaignPayload(BaseModel):
    agent_id: str
    list_id: str
    name: str
    description: str
    sender_phone_number: str
    delay_between_calls: int = 10  # Default 10 seconds delay
    scheduled_time: datetime
    timezone: str = "UTC"  # Default timezone

# Create Campaign

@app.post('/campaign')
async def create_campaign(
    payload: CreateCampaignPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            # First check if agent is inbound-only
            agent = await conn.fetchrow(
                'SELECT call_direction FROM agents WHERE agent_id = $1',
                uuid.UUID(payload.agent_id)
            )
            
            if not agent:
                raise HTTPException(status_code=404, detail="Agent not found")
                
            # Check if agent is inbound-only
            if agent['call_direction'] == 'inbound':
                raise HTTPException(
                    status_code=403, 
                    detail="This agent is configured for inbound calls only and cannot be used in outbound campaigns"
                )

            # Rest of the existing campaign creation logic
            contact_count = await conn.fetchval('''
                SELECT COUNT(*) FROM contacts 
                WHERE list_id = $1 AND account_id = $2
            ''', uuid.UUID(payload.list_id), account_id)
            
            if contact_count == 0:
                raise HTTPException(status_code=400, detail="Contact list is empty")
            
            # Convert scheduled time to UTC for storage
            local_tz = pytz.timezone(payload.timezone)
            utc_scheduled_time = local_tz.localize(payload.scheduled_time).astimezone(pytz.UTC)
            
            # Determine initial status
            current_time = datetime.now(pytz.UTC)
            initial_status = 'overdue' if utc_scheduled_time < current_time else 'pending'
            
            # Create campaign
            campaign_id = uuid.uuid4()
            await conn.execute('''
                INSERT INTO campaigns (
                    campaign_id, account_id, agent_id, list_id, 
                    name, description, sender_phone_number, total_contacts,
                    delay_between_calls, scheduled_time, timezone, status
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ''', campaign_id, account_id, uuid.UUID(payload.agent_id),
                uuid.UUID(payload.list_id), payload.name, payload.description,
                payload.sender_phone_number, contact_count, payload.delay_between_calls,
                utc_scheduled_time, payload.timezone, initial_status)
            
            # Schedule processing logic remains the same
            if initial_status == 'overdue':
                asyncio.create_task(process_campaign(campaign_id))
            else:
                scheduler.add_job(
                    process_campaign,
                    'date',
                    run_date=utc_scheduled_time,
                    args=[campaign_id],
                    id=str(campaign_id)
                )
            
            return {
                "campaign_id": str(campaign_id),
                "status": initial_status,
                "total_contacts": contact_count,
                "scheduled_time": utc_scheduled_time.isoformat(),
                "timezone": payload.timezone,
                "delay_between_calls": payload.delay_between_calls
            }
    except Exception as e:
        logger.error(f"Error creating campaign: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create campaign")

# Process Campaign

async def update_campaign_status(conn, campaign_id: uuid.UUID, new_status: str):
    """Helper function to update campaign status and track changes"""
    await conn.execute('''
        UPDATE campaigns 
        SET status = $1, 
            status_changed_at = CURRENT_TIMESTAMP 
        WHERE campaign_id = $2
    ''', new_status, campaign_id)

async def process_campaign(campaign_id: uuid.UUID):
    try:
        async with get_db_conn() as conn:
            # Update campaign status to running
            await update_campaign_status(conn, campaign_id, 'running')
            
            # Get campaign details
            campaign = await conn.fetchrow('''
                SELECT c.*, a.agent_id 
                FROM campaigns c
                JOIN agents a ON c.account_id = a.account_id
                WHERE c.campaign_id = $1
            ''', campaign_id)
            
            if not campaign:
                logger.error(f"Campaign {campaign_id} not found")
                return
            
            delay_seconds = campaign['delay_between_calls']
            
            while True:
                # Get next pending contact
                contact = await conn.fetchrow('''
                    SELECT contact_id, phone_number, template_variables 
                    FROM contacts 
                    WHERE list_id = $1 AND status = 'pending'
                    LIMIT 1
                ''', campaign['list_id'])
                
                if not contact:
                    # Check if campaign is complete
                    stats = await conn.fetchrow('''
                        SELECT 
                            COUNT(*) FILTER (WHERE status = 'completed') as completed,
                            COUNT(*) FILTER (WHERE status = 'failed') as failed,
                            COUNT(*) as total
                        FROM contacts 
                        WHERE list_id = $1
                    ''', campaign['list_id'])
                    
                    if stats['completed'] + stats['failed'] == stats['total']:
                        await update_campaign_status(conn, campaign_id, 'completed')
                    break
                
                try:
                    # Update contact status to in_progress
                    await conn.execute('''
                        UPDATE contacts 
                        SET status = 'in_progress'
                        WHERE contact_id = $1
                    ''', contact['contact_id'])
                    
                    # Prepare call details
                    call_details = {
                        'account_id': str(campaign['account_id']),
                        'agent_id': str(campaign['agent_id']),
                        'sender_phone_number': campaign['sender_phone_number'],
                        'recipient_phone_number': contact['phone_number'],
                        'template_variables': contact['template_variables']
                    }
                    
                    # Make the call
                    result = await make_campaign_call(call_details)
                    
                    if result['status'] == 'success':
                        # Update contact as completed
                        await conn.execute('''
                            UPDATE contacts 
                            SET status = 'completed',
                                call_sid = $1,
                                processed_at = CURRENT_TIMESTAMP
                            WHERE contact_id = $2
                        ''', result['call_sid'], contact['contact_id'])
                        
                        # Update campaign stats
                        await conn.execute('''
                            UPDATE campaigns 
                            SET completed_calls = completed_calls + 1
                            WHERE campaign_id = $1
                        ''', campaign_id)
                    
                    # Wait between calls
                    await asyncio.sleep(delay_seconds)
                    
                except Exception as e:
                    logger.error(f"Error processing contact {contact['contact_id']}: {str(e)}")
                    await conn.execute('''
                        UPDATE contacts 
                        SET status = 'failed',
                            error_message = $1,
                            processed_at = CURRENT_TIMESTAMP
                        WHERE contact_id = $2
                    ''', str(e), contact['contact_id'])
                    
                    await conn.execute('''
                        UPDATE campaigns 
                        SET failed_calls = failed_calls + 1
                        WHERE campaign_id = $1
                    ''', campaign_id)
                    
                    continue
                    
    except Exception as e:
        logger.error(f"Campaign processing error: {str(e)}")
        async with get_db_conn() as conn:
            await update_campaign_status(conn, campaign_id, 'failed')

async def make_campaign_call(call_details: dict):
    try:
        # Get the domain URLs
        app_callback_url, websocket_url = populate_custom_urls()
        
        async with get_db_conn() as conn:
            # Get subaccount info
            subaccount_info = await conn.fetchrow(
                'SELECT twilio_subaccount_sid, twilio_subaccount_auth_token FROM accounts WHERE account_id = $1',
                uuid.UUID(call_details['account_id'])
            )
            
            if not subaccount_info:
                raise Exception("Account not found")

            # Create Twilio client
            subaccount_client = Client(
                twilio_account_sid,
                twilio_auth_token,
                subaccount_info['twilio_subaccount_sid']
            )
            
            # Make the call
            call = subaccount_client.calls.create(
                to=call_details['recipient_phone_number'],
                from_=call_details['sender_phone_number'],
                url=f"{app_callback_url}/twilio_callback?ws_url={websocket_url}&agent_id={call_details['agent_id']}&account_id={call_details['account_id']}",
                record=not call_details['is_compliant'],  # Record only if agent is not compliant
                recording_status_callback=f"{app_callback_url}/recording_callback" if not call_details['is_compliant'] else None,
                recording_status_callback_event=['completed'] if not call_details['is_compliant'] else None,
                status_callback=f"{app_callback_url}/call_status_callback",
                status_callback_event=['completed']
            )
            
            return {
                "status": "success",
                "call_sid": call.sid
            }
            
    except Exception as e:
        logger.error(f"Error making campaign call: {str(e)}")
        raise



# Check for overdue campaigns

async def check_overdue_campaigns():
    try:
        async with get_db_conn() as conn:
            # Find campaigns that need processing, excluding paused campaigns
            overdue_campaigns = await conn.fetch('''
                SELECT campaign_id 
                FROM campaigns 
                WHERE status = 'overdue'
                AND status != 'paused'  -- Exclude paused campaigns
                AND scheduled_time < CURRENT_TIMESTAMP
            ''')
            
            # Check for pending campaigns that should become overdue
            # Explicitly exclude paused campaigns
            pending_campaigns = await conn.fetch('''
                SELECT campaign_id 
                FROM campaigns 
                WHERE status = 'pending'
                AND status != 'paused'  -- Exclude paused campaigns
                AND scheduled_time < CURRENT_TIMESTAMP
            ''')
            
            # Update only non-paused pending campaigns to overdue
            for campaign in pending_campaigns:
                # Double check campaign isn't paused before updating
                campaign_status = await conn.fetchval('''
                    SELECT status 
                    FROM campaigns 
                    WHERE campaign_id = $1
                ''', campaign['campaign_id'])
                
                if campaign_status != 'paused':
                    await update_campaign_status(conn, campaign['campaign_id'], 'overdue')
                    overdue_campaigns.append(campaign)
            
            # Process overdue campaigns
            for campaign in overdue_campaigns:
                # Final check to ensure campaign isn't paused
                campaign_status = await conn.fetchval('''
                    SELECT status 
                    FROM campaigns 
                    WHERE campaign_id = $1
                ''', campaign['campaign_id'])
                
                if campaign_status != 'paused':
                    await conn.execute('''
                        UPDATE campaigns 
                        SET completed_calls = 0,
                            failed_calls = 0,
                            started_at = NULL,
                            completed_at = NULL,
                            status_changed_at = CURRENT_TIMESTAMP
                        WHERE campaign_id = $1 
                        AND completed_at IS NOT NULL
                    ''', campaign['campaign_id'])
                    
                    asyncio.create_task(process_campaign(campaign['campaign_id']))
                
    except Exception as e:
        logger.error(f"Error checking overdue campaigns: {str(e)}")

# Add the periodic check to the scheduler
scheduler.add_job(
    check_overdue_campaigns,
    'interval',
    minutes=1,
    id='check_overdue_campaigns'
)
    


# Campaign Status

@app.get('/campaign/{campaign_id}')
async def get_campaign_status(
    campaign_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            campaign = await conn.fetchrow('''
                SELECT 
                    campaign_id, name, status, total_contacts, 
                    completed_calls, failed_calls, created_at, 
                    started_at, completed_at, scheduled_time, timezone
                FROM campaigns 
                WHERE campaign_id = $1 AND account_id = $2
            ''', campaign_id, account_id)
            
            if not campaign:
                raise HTTPException(status_code=404, detail="Campaign not found")
            
            # Convert UTC time to local timezone for response
            local_tz = pytz.timezone(campaign['timezone'])
            scheduled_local = campaign['scheduled_time'].astimezone(local_tz)
            
            return {
                "campaign_id": str(campaign['campaign_id']),
                "name": campaign['name'],
                "status": campaign['status'],
                "progress": {
                    "total": campaign['total_contacts'],
                    "completed": campaign['completed_calls'],
                    "failed": campaign['failed_calls'],
                    "pending": campaign['total_contacts'] - (campaign['completed_calls'] + campaign['failed_calls'])
                },
                "timing": {
                    "created_at": campaign['created_at'].isoformat(),
                    "scheduled_for": scheduled_local.isoformat(),
                    "started_at": campaign['started_at'].isoformat() if campaign['started_at'] else None,
                    "completed_at": campaign['completed_at'].isoformat() if campaign['completed_at'] else None
                },
                "timezone": campaign['timezone']
            }
            
    except Exception as e:
        logger.error(f"Error fetching campaign status: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch campaign status")    
    


# Campaign Management
    
@app.post('/campaign/pause/{campaign_id}')
async def pause_campaign(
    campaign_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            # Check if campaign exists and belongs to account
            campaign = await conn.fetchrow(
                'SELECT status FROM campaigns WHERE campaign_id = $1 AND account_id = $2',
                campaign_id, account_id
            )
            
            if not campaign:
                raise HTTPException(status_code=404, detail="Campaign not found")
            
            # Set a Redis flag to stop the campaign processing
            await redis_client.set(f"campaign_paused:{campaign_id}", "1", ex=86400)  # 24hr expiry
            
            # Update status to paused
            await conn.execute('''
                UPDATE campaigns 
                SET status = 'paused',
                    status_changed_at = CURRENT_TIMESTAMP
                WHERE campaign_id = $1
            ''', campaign_id)
            
            # If there's a scheduled job, remove it
            if scheduler.get_job(str(campaign_id)):
                scheduler.remove_job(str(campaign_id))
            
            return {
                "status": "success",
                "message": "Campaign paused successfully. Active calls will complete but no new calls will be initiated.",
                "campaign_id": str(campaign_id)
            }
            
    except Exception as e:
        logger.error(f"Error pausing campaign: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to pause campaign")

@app.post('/campaign/resume/{campaign_id}')
async def resume_campaign(
    campaign_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            # Remove the pause flag from Redis
            await redis_client.delete(f"campaign_paused:{campaign_id}")
            
            # Check if campaign exists and belongs to account
            campaign = await conn.fetchrow(
                'SELECT status, scheduled_time FROM campaigns WHERE campaign_id = $1 AND account_id = $2',
                campaign_id, account_id
            )
            
            if not campaign:
                raise HTTPException(status_code=404, detail="Campaign not found")
            
            if campaign['status'] != 'paused':
                raise HTTPException(status_code=400, detail="Campaign is not paused")
            
            # Determine new status based on scheduled time
            current_time = datetime.now(pytz.UTC)
            new_status = 'overdue' if campaign['scheduled_time'] < current_time else 'pending'
            
            # Update campaign status
            await conn.execute('''
                UPDATE campaigns 
                SET status = $1,
                    status_changed_at = CURRENT_TIMESTAMP
                WHERE campaign_id = $2
            ''', new_status, campaign_id)
            
            # If status is overdue, start processing immediately
            if new_status == 'overdue':
                asyncio.create_task(process_campaign(campaign_id))
            elif new_status == 'pending':
                # Schedule the campaign for future execution
                scheduler.add_job(
                    process_campaign,
                    'date',
                    run_date=campaign['scheduled_time'],
                    args=[campaign_id],
                    id=str(campaign_id)
                )
            
            return {
                "status": "success",
                "message": f"Campaign resumed successfully. New status: {new_status}",
                "campaign_id": str(campaign_id)
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error resuming campaign: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to resume campaign")



# Delete Campaign

@app.delete('/campaign/{campaign_id}')
async def delete_campaign(
    campaign_id: uuid.UUID,
    delete_contact_list: bool = Query(False),
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            async with conn.transaction():
                # Get campaign details first
                campaign = await conn.fetchrow(
                    'SELECT list_id, status FROM campaigns WHERE campaign_id = $1',
                    campaign_id
                )
                
                if not campaign:
                    raise HTTPException(status_code=404, detail="Campaign not found")
                
                # Clean up Redis flags
                await redis_client.delete(f"campaign_paused:{campaign_id}")
                
                # Cancel any scheduled jobs
                if scheduler.get_job(str(campaign_id)):
                    scheduler.remove_job(str(campaign_id))
                
                # Delete all campaign-related data
                await conn.execute(
                    'DELETE FROM campaigns WHERE campaign_id = $1',
                    campaign_id
                )
                
                # If requested and campaign has an associated contact list, delete it
                if delete_contact_list and campaign['list_id']:
                    # First delete contacts
                    await conn.execute(
                        'DELETE FROM contacts WHERE list_id = $1',
                        campaign['list_id']
                    )
                    # Then delete the contact list
                    await conn.execute(
                        'DELETE FROM contact_lists WHERE list_id = $1',
                        campaign['list_id']
                    )
                
                return {
                    "status": "success",
                    "message": "Campaign deleted successfully",
                    "campaign_id": str(campaign_id),
                    "previous_status": campaign['status'],
                    "contact_list_deleted": delete_contact_list
                }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting campaign: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete campaign")


# Edit Campaign
    

class EditCampaignPayload(BaseModel):
    account_id: Optional[str] = None
    agent_id: Optional[str] = None
    list_id: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    sender_phone_number: Optional[str] = None
    delay_between_calls: Optional[int] = None
    scheduled_time: Optional[datetime] = None
    timezone: Optional[str] = None
    record: Optional[bool] = None

@app.patch('/campaign/{campaign_id}')
async def edit_campaign(
    campaign_id: uuid.UUID,
    payload: EditCampaignPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            # Check if campaign exists and belongs to account
            campaign = await conn.fetchrow(
                'SELECT status, account_id FROM campaigns WHERE campaign_id = $1 AND account_id = $2',
                campaign_id, account_id
            )
            
            if not campaign:
                raise HTTPException(status_code=404, detail="Campaign not found")
            
            # If list_id is being updated, verify it has contacts
            if payload.list_id:
                # Use the existing account_id if not provided in payload
                account_id = uuid.UUID(payload.account_id) if payload.account_id else campaign['account_id']
                
                contact_count = await conn.fetchval('''
                    SELECT COUNT(*) FROM contacts 
                    WHERE list_id = $1 AND account_id = $2
                ''', uuid.UUID(payload.list_id), account_id)
                
                if contact_count == 0:
                    raise HTTPException(status_code=400, detail="New contact list is empty")

            # Rest of the function remains the same
            update_fields = []
            params = []
            param_count = 1

            field_mappings = {
                'account_id': ('account_id', uuid.UUID),
                'agent_id': ('agent_id', uuid.UUID),
                'list_id': ('list_id', uuid.UUID),
                'name': ('name', str),
                'description': ('description', str),
                'sender_phone_number': ('sender_phone_number', str),
                'delay_between_calls': ('delay_between_calls', int),
                'scheduled_time': ('scheduled_time', datetime),
                'timezone': ('timezone', str),
                'record': ('record', bool)
            }

            for field, value in payload.dict(exclude_unset=True).items():
                if value is not None:
                    db_field, type_conv = field_mappings[field]
                    update_fields.append(f"{db_field} = ${param_count}")
                    params.append(type_conv(value) if field in ['account_id', 'agent_id', 'list_id'] else value)
                    param_count += 1

            if update_fields:
                # Add status_changed_at to update
                update_fields.append("status_changed_at = CURRENT_TIMESTAMP")
                
                # Construct and execute update query
                query = f"""
                    UPDATE campaigns 
                    SET {', '.join(update_fields)}
                    WHERE campaign_id = ${param_count}
                    RETURNING list_id, status
                """
                params.append(campaign_id)
                
                updated_campaign = await conn.fetchrow(query, *params)

                # Update scheduler if scheduled_time was changed
                if payload.scheduled_time:
                    scheduled_utc = payload.scheduled_time.astimezone(pytz.UTC)
                    if scheduler.get_job(str(campaign_id)):
                        scheduler.remove_job(str(campaign_id))
                    
                    scheduler.add_job(
                        process_campaign,
                        'date',
                        run_date=scheduled_utc,
                        args=[campaign_id],
                        id=str(campaign_id)
                    )

            return {
                "status": "success",
                "message": "Campaign updated successfully",
                "campaign_id": str(campaign_id),
                "list_id": str(updated_campaign["list_id"]),
                "previous_status": updated_campaign["status"]
            }
            
    except Exception as e:
        logger.error(f"Error updating campaign: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to update campaign")
    


# Get All Campaigns


@app.get('/campaigns')
async def get_campaigns(
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            campaigns = await conn.fetch('''
                SELECT 
                    c.*,
                    a.name as agent_name,
                    cl.name as contact_list_name,
                    CASE 
                        WHEN c.status = 'completed' THEN 'completed'
                        WHEN c.status = 'paused' THEN 'paused'
                        WHEN c.status = 'active' AND c.scheduled_time > CURRENT_TIMESTAMP THEN 'scheduled'
                        WHEN c.status = 'active' AND c.scheduled_time <= CURRENT_TIMESTAMP THEN 'running'
                        ELSE c.status
                    END as campaign_status
                FROM campaigns c
                LEFT JOIN agents a ON c.agent_id = a.agent_id
                LEFT JOIN contact_lists cl ON c.list_id = cl.list_id
                WHERE c.account_id = $1
                ORDER BY c.created_at DESC
            ''', account_id)
            
            return {
                "campaigns": [
                    {
                        **dict(campaign),
                        "campaign_id": str(campaign["campaign_id"]),
                        "account_id": str(campaign["account_id"]),
                        "scheduled_time": campaign["scheduled_time"].isoformat(),
                        "timezone": campaign["timezone"],
                        "list_id": str(campaign["list_id"]),
                        "agent_id": str(campaign["agent_id"]),
                        "agent_name": campaign["agent_name"],
                        "contact_list_name": campaign["contact_list_name"],
                        "created_at": campaign["created_at"].isoformat(),
                        "started_at": campaign["started_at"].isoformat() if campaign["started_at"] else None,
                        "completed_at": campaign["completed_at"].isoformat() if campaign["completed_at"] else None,
                        "status_changed_at": campaign["status_changed_at"].isoformat() if campaign["status_changed_at"] else None,
                        "campaign_status": campaign["campaign_status"],
                        "progress": {
                            "total": campaign["total_contacts"],
                            "completed": campaign["completed_calls"],
                            "failed": campaign["failed_calls"],
                            "pending": campaign["total_contacts"] - (campaign["completed_calls"] + campaign["failed_calls"])
                        }
                    }
                    for campaign in campaigns
                ]
            }
            
    except Exception as e:
        logger.error(f"Error fetching campaigns: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch campaigns")
    





##############################################
# Inbound Agent Calls
#############################################


@app.post('/inbound/webhook')
async def handle_inbound_call(request: Request):
    try:
        form_data = await request.form()
        from_number = form_data.get('From')
        to_number = form_data.get('To')
        call_sid = form_data.get('CallSid')
        
        # Look up the agent ID from the database instead of Redis
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            agent = await conn.fetchrow('''
                SELECT agent_id, call_direction, account_id, is_compliant 
                FROM agents 
                WHERE inbound_phone_number = $1
                  AND call_direction = 'inbound'
            ''', to_number)

        if not agent:
            response = VoiceResponse()
            response.say("No agent is configured for this number.")
            return PlainTextResponse(str(response), media_type='text/xml')

        # Get the domain URLs
        app_callback_url, websocket_url = populate_custom_urls()

        # Record the call in the database
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    'INSERT INTO calls (call_sid, agent_id, account_id, from_number, to_number, call_type) VALUES ($1, $2, $3, $4, $5, $6)',
                    call_sid, agent['agent_id'], agent['account_id'], from_number, to_number, 'inbound'
                )
                await conn.execute(
                    'UPDATE agents SET total_calls = total_calls + 1 WHERE agent_id = $1',
                    agent['agent_id']
                )
                await conn.execute(
                    'UPDATE accounts SET total_calls = total_calls + 1 WHERE account_id = $1',
                    agent['account_id']
                )

        # Create TwiML response
        response = VoiceResponse()
        
        # Redirect to callback with all necessary parameters
        response.redirect(
            f"{app_callback_url}/inbound_twilio_callback?ws_url={websocket_url}&agent_id={agent['agent_id']}&account_id={agent['account_id']}",
            method='POST'
        )

        # Add status and recording callbacks
        response.status_callback = f"{app_callback_url}/inbound_call_callback"
        response.status_callback_event = ['completed', 'failed']
        response.status_callback_method = 'POST'
        response.recording_status_callback = f"{app_callback_url}/inbound_call_callback"
        response.recording_status_callback_event = ['completed']
        response.recording_status_callback_method = 'POST'

        # Add recording callback only if agent is not compliant
        if not agent['is_compliant']:
            response.recording_status_callback = f"{app_callback_url}/inbound_call_callback"
            response.recording_status_callback_event = ['completed']
            response.recording_status_callback_method = 'POST'
            response.record()  # Enable recording

        return PlainTextResponse(str(response), media_type='text/xml')

    except Exception as e:
        logger.error(f"Error handling inbound call: {e}")
        response = VoiceResponse()
        response.say("An error occurred processing your call.")
        return PlainTextResponse(str(response), media_type='text/xml')







# Twilio Callback

@app.post('/inbound_twilio_callback')
async def inbound_twilio_callback(ws_url: str = Query(...), agent_id: str = Query(...), account_id: str = Query(...)):
    try:
        response = VoiceResponse()

        connect = Connect()
        websocket_twilio_route = f'{ws_url}/v1/chat/{agent_id}'
        connect.stream(url=websocket_twilio_route)
        print(f"websocket connection done to {websocket_twilio_route}")
        response.append(connect)

        return PlainTextResponse(str(response), status_code=200, media_type='text/xml')

    except Exception as e:
        print(f"Exception occurred in twilio_callback: {e}")
        
        
        
        
# Inbound Call Callback

@app.post('/inbound_call_callback')
async def inbound_call_callback(request: Request):
    try:
        form_data = await request.form()
        call_sid = form_data.get('CallSid')
        call_status = form_data.get('CallStatus')
        call_duration = form_data.get('CallDuration')
        transcription_json_str = None

        # Get transcription from Redis if available
        if call_status == 'completed':
            redis_key = f"transcription:{call_sid}"
            transcription_json_str = await redis_client.get(redis_key)

        async with get_db_conn() as conn:
            # Get call details from database
            call_details = await conn.fetchrow(
                'SELECT agent_id, account_id, from_number, to_number FROM calls WHERE call_sid = $1',
                call_sid
            )

            if call_details:
                # Calculate call cost using our pricing function
                call_cost = 0
                if call_duration and call_status == 'completed':
                    call_cost = await calculate_call_cost(
                        int(call_duration), 
                        call_details['account_id']
                    )

                # Update call record with duration, status, call_type and cost
                await conn.execute('''
                    UPDATE calls 
                    SET status = $1,
                        duration = $2,
                        transcription = $3::jsonb,
                        call_type = 'inbound',
                        cost = $4
                    WHERE call_sid = $5
                ''', call_status, float(call_duration) if call_duration else 0, 
                    transcription_json_str if transcription_json_str else None,
                    call_cost,
                    call_sid)
                
                # Update agent's total duration and cost
                await conn.execute('''
                    UPDATE agents 
                    SET total_duration = total_duration + $1,
                        total_cost = total_cost + $2
                    WHERE agent_id = $3
                ''', float(call_duration) if call_duration else 0, 
                    call_cost,
                    call_details['agent_id'])

                # Update account stats
                await conn.execute('''
                    UPDATE accounts 
                    SET total_duration = total_duration + $1,
                        total_cost = total_cost + $2
                    WHERE account_id = $3
                ''', float(call_duration) if call_duration else 0,
                    call_cost,
                    call_details['account_id'])

                # Send webhook notification
                webhook_payload = {
                    "event": "call_ended",
                    "call_sid": call_sid,
                    "status": call_status,
                    "duration": float(call_duration) if call_duration else 0,
                    "cost": call_cost,
                    "from_number": call_details['from_number'],
                    "to_number": call_details['to_number'],
                    "transcription": json.loads(transcription_json_str) if transcription_json_str else None
                }
                await notify_agent_webhook(call_details['agent_id'], webhook_payload)
                
                # Delete from Redis after successful DB update
                if transcription_json_str:
                    await redis_client.delete(redis_key)

        return PlainTextResponse("OK")
    except Exception as e:
        logger.error(f"Error in inbound_call_callback: {str(e)}")
        return PlainTextResponse("Error", status_code=500)










################################################################################################################################################################################


##############################################################
# Twilio Number Management
##############################################################

# search twilio numbers

@app.get("/available-numbers")
async def get_available_numbers(
    country: str = Query("US", description="Country code (e.g., US, GB, CA)"),
    number_type: str = Query("local", description="Number type (local, mobile, toll-free, national)"),
    area_code: str = Query(None, description="Area code to search for"),
    limit: int = Query(20, description="Number of results to return"),
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        numbers = twilio_client.available_phone_numbers(country)
        
        kwargs = {"limit": limit}
        if area_code:
            kwargs["area_code"] = area_code

        if number_type == "local" or number_type == "national":
            available_numbers = numbers.local.list(**kwargs)
        elif number_type == "mobile":
            available_numbers = numbers.mobile.list(**kwargs)
        elif number_type == "national":
            available_numbers = numbers.national.list(**kwargs)
        elif number_type == "toll-free":
            available_numbers = numbers.toll_free.list(**kwargs)
        else:
            raise HTTPException(status_code=400, detail=f"Invalid number type: {number_type}")

        formatted_numbers = [
            {
                "friendly_name": number.friendly_name,
                "phone_number": number.phone_number,
                "lata": number.lata,
                "locality": number.locality,
                "rate_center": number.rate_center,
                "latitude": number.latitude,
                "longitude": number.longitude,
                "region": number.region,
                "postal_code": number.postal_code,
                "iso_country": number.iso_country,
                "capabilities": {
                    "voice": number.capabilities.get("voice", False),
                    "sms": number.capabilities.get("sms", False),
                    "mms": number.capabilities.get("mms", False),
                }
            }
            for number in available_numbers
        ]

        response = {"available_phone_numbers": formatted_numbers}
        logger.debug(f"Returning response: {response}")
        return response

    except TwilioRestException as e:
        logger.error(f"Twilio API error: {str(e)}")
        raise HTTPException(status_code=e.status, detail=f"Twilio API error: {str(e)}")
    except Exception as e:
        logger.error(f"Internal server error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")





# Get numbers owned by sub account

@app.get("/account-phone-numbers")
async def get_account_phone_numbers(
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Get phone numbers with subscription info
            phone_numbers = await conn.fetch('''
                SELECT p.*, u."accountId" 
                FROM "PhoneNumber" p
                JOIN "User" u ON u."clerkId" = p."userId"
                WHERE u."accountId"::uuid = $1
            ''', account_id)
            
            # Get Twilio credentials
            subaccount_info = await conn.fetchrow(
                'SELECT twilio_subaccount_sid, twilio_subaccount_auth_token FROM accounts WHERE account_id = $1',
                account_id
            )
        
        if not subaccount_info:
            raise HTTPException(status_code=404, detail="Account not found")

        subaccount_client = Client(subaccount_info['twilio_subaccount_sid'], subaccount_info['twilio_subaccount_auth_token'])
        incoming_phone_numbers = subaccount_client.incoming_phone_numbers.list(limit=20)
        
        formatted_numbers = []
        for number in incoming_phone_numbers:
            # Find matching DB record
            db_number = next((n for n in phone_numbers if n['sid'] == number.sid), None)
            
            if db_number and db_number['stripeSubscriptionId']:
                # Get subscription details from Stripe
                try:
                    subscription = stripe.Subscription.retrieve(db_number['stripeSubscriptionId'])
                    renewal_date = datetime.fromtimestamp(subscription.current_period_end)
                    renewal_formatted = renewal_date.strftime("%B %d, %Y")
                except stripe.error.StripeError:
                    renewal_formatted = "Unknown"
            else:
                renewal_formatted = "Not subscribed"

            formatted_numbers.append({
                "sid": number.sid,
                "phone_number": number.phone_number,
                "friendly_name": number.friendly_name,
                "capabilities": number.capabilities,
                "status": "inbound" if number.sms_url or number.voice_url else "outbound",
                "cost": "$4.99/month",
                "renewal_date": renewal_formatted
            })
        
        return {"phone_numbers": formatted_numbers}
    except TwilioRestException as e:
        logger.error(f"Twilio API error: {str(e)}")
        raise HTTPException(status_code=e.status, detail=f"Twilio API error: {str(e)}")
    except Exception as e:
        logger.error(f"Internal server error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    






# purchase a number

class PurchaseNumberRequest(BaseModel):
    phone_number: str
    friendly_name: Optional[str] = None
    payment_method_id: str

@app.post("/purchase-number")
async def purchase_phone_number(
    request: PurchaseNumberRequest,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        # Get user info and subaccount credentials
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Get both user info and subaccount credentials in one query
            account_info = await conn.fetchrow('''
                SELECT 
                    u."clerkId",
                    a.twilio_subaccount_sid,
                    a.twilio_subaccount_auth_token,
                    (SELECT "stripeCustomerId" FROM "User" WHERE "accountId"::uuid = $1::uuid) as stripe_customer_id
                FROM accounts a
                JOIN "User" u ON u."accountId"::uuid = a.account_id
                WHERE a.account_id = $1
            ''', account_id)

        if not account_info:
            raise HTTPException(status_code=404, detail="Account not found")
            
        # Create Twilio client with subaccount credentials
        client = Client(
            account_info['twilio_subaccount_sid'],
            account_info['twilio_subaccount_auth_token']
        )

        # Check availability using subaccount
        available_numbers = client.available_phone_numbers('US') \
            .local \
            .list(contains=request.phone_number.replace('+', ''))

        if not available_numbers:
            raise HTTPException(status_code=400, detail="Phone number is not available")

        # Handle Stripe subscription (rest of the code remains the same)
        stripe_customer_id = account_info['stripe_customer_id']
        if not stripe_customer_id:
            raise HTTPException(status_code=400, detail="No payment method configured")

        # Create subscription first
        try:
            subscription = stripe.Subscription.create(
                customer=stripe_customer_id,
                items=[{"price": "price_1Qvp9JIOT2rIrW690k2XkIAi"}],
                payment_behavior='default_incomplete',
                payment_settings={'save_default_payment_method': 'on_subscription'},
                metadata={
                    'phone_number': request.phone_number,
                    'account_id': str(account_id)
                },
                expand=['latest_invoice.payment_intent']
            )
        except stripe.error.StripeError as e:
            raise HTTPException(status_code=400, detail=f"Subscription creation failed: {str(e)}")

        # Only purchase number if subscription succeeds
        purchased_number = client.incoming_phone_numbers.create(
            phone_number=request.phone_number,
            friendly_name=request.friendly_name
        )

        # Save to database with subscription info
        async with pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO "PhoneNumber" 
                ("id", "phoneNumber", "friendlyName", "sid", "userId", "stripeSubscriptionId", "createdAt", "updatedAt")
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ''', 
                str(uuid.uuid4()), 
                purchased_number.phone_number,
                request.friendly_name,
                purchased_number.sid,
                user_info['clerkId'],
                subscription.id
            )

        return {
            "success": True,
            "phone_number": purchased_number.phone_number,
            "sid": purchased_number.sid,
            "subscription_id": subscription.id
        }

    except TwilioRestException as e:
        logger.error(f"Twilio API error: {str(e)}")
        raise HTTPException(status_code=e.status, detail=str(e))
    except Exception as e:
        logger.error(f"Internal server error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")




# cancel a number

@app.post("/cancel-number")
async def cancel_phone_number(
    sid: str,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Get phone number info including subscription ID
            phone_number = await conn.fetchrow('''
                SELECT p.*, u."accountId" 
                FROM "PhoneNumber" p
                JOIN "User" u ON u."clerkId" = p."userId"
                WHERE p.sid = $1 AND u."accountId"::uuid = $2
            ''', sid, account_id)
            
            if not phone_number:
                raise HTTPException(status_code=403, detail="You don't own this phone number")

            # Cancel Stripe subscription if it exists
            if phone_number['stripeSubscriptionId']:
                try:
                    stripe.Subscription.delete(phone_number['stripeSubscriptionId'])
                except stripe.error.StripeError as e:
                    logger.error(f"Error canceling Stripe subscription: {str(e)}")
                    # Continue with number cancellation even if subscription cancellation fails

            # Get Twilio credentials and delete number
            subaccount_info = await conn.fetchrow(
                'SELECT twilio_subaccount_sid, twilio_subaccount_auth_token FROM accounts WHERE account_id = $1',
                account_id
            )

            if not subaccount_info:
                raise HTTPException(status_code=400, detail="Account not found or missing Twilio credentials")

            # Initialize Twilio client and delete number
            client = Client(
                subaccount_info['twilio_subaccount_sid'],
                subaccount_info['twilio_subaccount_auth_token']
            )
            client.incoming_phone_numbers(sid).delete()

            # Delete from our database
            await conn.execute(
                'DELETE FROM "PhoneNumber" WHERE sid = $1',
                sid
            )

        return {"success": True}

    except TwilioRestException as e:
        logger.error(f"Twilio API error: {str(e)}")
        raise HTTPException(status_code=e.status, detail=str(e))
    except Exception as e:
        logger.error(f"Internal server error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



# Update a number

router = APIRouter()

class UpdateNumberPayload(BaseModel):
    sid: str
    friendly_name: str

@router.post("/update-number")
async def update_phone_number(
    payload: UpdateNumberPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        # Get Twilio credentials from account
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            # Verify ownership first
            ownership = await conn.fetchrow(
                'SELECT 1 FROM "PhoneNumber" WHERE sid = $1 AND account_id = $2',
                payload.sid, account_id
            )
            if not ownership:
                raise HTTPException(status_code=403, detail="You don't own this phone number")

            subaccount_info = await conn.fetchrow(
                'SELECT twilio_subaccount_sid, twilio_subaccount_auth_token FROM accounts WHERE account_id = $1',
                account_id
            )
        
        if not subaccount_info or not subaccount_info['twilio_subaccount_sid'] or not subaccount_info['twilio_subaccount_auth_token']:
            raise HTTPException(status_code=400, detail="Account not found or missing Twilio credentials")

        # Initialize Twilio client
        client = Client(
            subaccount_info['twilio_subaccount_sid'],
            subaccount_info['twilio_subaccount_auth_token']
        )

        # Update the phone number in Twilio
        updated_number = client.incoming_phone_numbers(payload.sid).update(
            friendly_name=payload.friendly_name
        )

        # Update the phone number in our database
        async with pool.acquire() as conn:
            await conn.execute(
                'UPDATE "PhoneNumber" SET friendly_name = $1 WHERE sid = $2 AND account_id = $3',
                payload.friendly_name, payload.sid, account_id
            )

        return {
            "success": True,
            "phone_number": updated_number.phone_number,
            "friendly_name": updated_number.friendly_name
        }

    except TwilioRestException as e:
        logger.error(f"Twilio API error: {str(e)}")
        raise HTTPException(status_code=e.status, detail=f"Twilio API error: {str(e)}")
    except Exception as e:
        logger.error(f"Internal server error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    
    
    
# Notify agent webhook (make.com or zapier or workflow builder)
async def notify_agent_webhook(agent_id: uuid.UUID, payload: dict):
    try:
        async with get_db_conn() as conn:
            # Get agent's webhook URL
            agent = await conn.fetchrow(
                'SELECT webhook_url FROM agents WHERE agent_id = $1',
                agent_id
            )
            
            if agent and agent['webhook_url']:
                async with aiohttp.ClientSession() as session:
                    async with session.post(agent['webhook_url'], json=payload) as response:
                        if response.status != 200:
                            logger.error(f"Webhook notification failed: {response.status}")
    except Exception as e:
        logger.error(f"Error in webhook notification: {str(e)}")






async def get_user_price_per_minute(conn, account_id: uuid.UUID) -> float:
    """Get the user's price per minute from User table"""
    user = await conn.fetchrow('''
        SELECT 
            custom_price_per_minute,
            default_price_per_minute,
            "planType"
        FROM "User" 
        WHERE "accountId" = $1
    ''', str(account_id))  # Convert UUID to string since accountId is String in User table

    if not user:
        return 0.20  # Default price if user not found
        
    if user['custom_price_per_minute'] is not None:
        return user['custom_price_per_minute']
    
    return user['default_price_per_minute']



async def calculate_call_cost(duration: int, account_id: uuid.UUID) -> float:
    """Calculate call cost based on exact duration and user's pricing"""
    async with get_db_conn() as conn:
        price_per_minute = await get_user_price_per_minute(conn, account_id)
        # Convert duration to minutes with decimal precision
        duration_minutes = duration / 60.0
        return duration_minutes * price_per_minute