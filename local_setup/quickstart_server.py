import os
import asyncio
import uuid
import traceback
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Query, Body, UploadFile, File, Depends, Form, Security
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis
from dotenv import load_dotenv
from bolna.helpers.utils import store_file
from bolna.prompts import *
from bolna.helpers.logger_config import configure_logger
from bolna.models import *
from bolna.llms import LiteLLM
from bolna.agent_manager.assistant_manager import AssistantManager
from bolna.helpers.data_ingestion_pipe import create_table, ingestion_task, ingestion_tasks
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import tempfile
import threading 
import asyncpg
import json
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List, Any
from twilio.rest import Client
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.redis import RedisJobStore
import os
import json
import requests
import uuid
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.rest import Client
from dotenv import load_dotenv
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query, Request, Path
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
import secrets
import hmac
import hashlib
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
from contextlib import asynccontextmanager

load_dotenv()
logger = configure_logger(__name__)

redis_pool = redis.ConnectionPool.from_url(os.getenv('REDIS_URL'), decode_responses=True)
redis_client = redis.Redis.from_pool(redis_pool)
active_websockets: List[WebSocket] = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db_pool()
    
    yield  # Server is running and handling requests
    
    # Shutdown
    global _pool
    if _pool:
        await _pool.close()

# Then initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

redis_pool = redis.ConnectionPool.from_url(os.getenv('REDIS_URL'), decode_responses=True)
redis_client = redis.Redis.from_pool(redis_pool)
# Initialize scheduler
scheduler = AsyncIOScheduler()
scheduler.start()

twilio_account_sid = os.getenv('TWILIO_ACCOUNT_SID')
twilio_auth_token = os.getenv('TWILIO_AUTH_TOKEN')
twilio_phone_number = os.getenv('TWILIO_PHONE_NUMBER')

# Initialize Twilio client
twilio_client = Client(twilio_account_sid, twilio_auth_token)

# Global pool variable
_pool: Optional[asyncpg.Pool] = None

async def init_db_pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            os.getenv('DATABASE_URL'),
            min_size=2,
            max_size=500,  # Conservative value for Azure's 429 limit
            max_inactive_connection_lifetime=60.0,  # Close inactive connections after 1 minute
            command_timeout=60
        )
    return _pool

async def get_db_pool():
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

# Add to FastAPI app startup/shutdown
async def shutdown():
    global _pool
    if _pool:
        await _pool.close()

def populate_custom_urls():
    """Returns custom domain URLs for HTTP and WebSocket connections"""
    app_callback_url = "https://api.kallabot.com"
    websocket_url = "wss://ws.kallabot.com"
    return app_callback_url, websocket_url

# Create tables if they don't exist
async def create_tables():
    pool = await get_db_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS accounts (
                account_id UUID PRIMARY KEY,
                number_of_agents INT DEFAULT 0,
                total_calls INT DEFAULT 0,
                total_duration FLOAT DEFAULT 0,
                total_cost FLOAT DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                twilio_subaccount_sid TEXT,
                twilio_subaccount_auth_token TEXT,
                api_key TEXT UNIQUE,
                api_key_created_at TIMESTAMP WITH TIME ZONE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS agents (
                agent_id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                name TEXT NOT NULL,
                agent_type TEXT,
                call_direction TEXT DEFAULT 'outbound',
                inbound_phone_number TEXT,
                total_calls INT DEFAULT 0,
                total_duration FLOAT DEFAULT 0,
                total_cost FLOAT DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                timezone VARCHAR(50) DEFAULT 'America/Los_Angeles',
                country VARCHAR(50) DEFAULT 'US',
                template_variables JSONB,
                agent_config JSONB,
                agent_prompts JSONB,
                agent_image TEXT,
                webhook_url TEXT,
                is_compliant BOOLEAN DEFAULT FALSE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS calls (
                call_sid TEXT PRIMARY KEY,
                agent_id UUID REFERENCES agents(agent_id),
                account_id UUID REFERENCES accounts(account_id),
                from_number TEXT NOT NULL,
                to_number TEXT NOT NULL,
                duration FLOAT DEFAULT 0,
                cost FLOAT DEFAULT 0,
                status VARCHAR(50) DEFAULT 'pending',
                call_type VARCHAR(10) DEFAULT 'outbound' CHECK (call_type IN ('inbound', 'outbound')),
                recording_url TEXT,
                transcription JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS contact_lists (
                list_id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                contact_id UUID PRIMARY KEY,
                list_id UUID REFERENCES contact_lists(list_id),
                account_id UUID REFERENCES accounts(account_id),
                phone_number TEXT NOT NULL,
                template_variables JSONB,
                status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed', 'in_progress')),
                call_sid TEXT,
                error_message TEXT,
                processed_at TIMESTAMP WITH TIME ZONE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS campaigns (
                campaign_id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                agent_id UUID REFERENCES agents(agent_id),
                list_id UUID REFERENCES contact_lists(list_id),
                name TEXT NOT NULL,
                description TEXT,
                status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'overdue', 'paused', 'cancelled')),
                sender_phone_number TEXT NOT NULL,
                total_contacts INT DEFAULT 0,
                completed_calls INT DEFAULT 0,
                failed_calls INT DEFAULT 0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                delay_between_calls INT DEFAULT 10,
                scheduled_time TIMESTAMP WITH TIME ZONE,
                timezone VARCHAR(50) DEFAULT 'UTC',
                status_changed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS knowledgebases (
                kb_id UUID PRIMARY KEY,
                account_id UUID REFERENCES accounts(account_id),
                friendly_name TEXT NOT NULL,
                vector_store_id TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_type TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'processing',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (account_id, friendly_name)
            )
        ''')

# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

twilio_client = Client(os.getenv('TWILIO_ACCOUNT_SID'), os.getenv('TWILIO_AUTH_TOKEN'))

######################################
# Account Management
######################################

# Replace the API key header with Bearer token security
security = HTTPBearer()

def generate_api_key() -> str:
    """Generate a secure API key."""
    return f"sk_{secrets.token_urlsafe(32)}"

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

# Create Account
@app.post("/v1/account")
async def create_account():
    async with get_db_conn() as conn:
        async with conn.transaction():
            account_id = uuid.uuid4()
            api_key = generate_api_key()
            
            # Create Twilio subaccount
            try:
                subaccount = twilio_client.api.v2010.accounts.create(friendly_name=f"Subaccount {account_id}")
                twilio_subaccount_sid = subaccount.sid
                twilio_subaccount_auth_token = subaccount.auth_token
            except Exception as e:
                logger.error(f"Error creating Twilio subaccount: {str(e)}")
                raise HTTPException(status_code=500, detail="Failed to create Twilio subaccount")

            # Insert account into database
            await conn.execute('''
                INSERT INTO accounts (
                    account_id, 
                    twilio_subaccount_sid, 
                    twilio_subaccount_auth_token,
                    api_key,
                    api_key_created_at
                ) VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)
            ''', account_id, twilio_subaccount_sid, twilio_subaccount_auth_token, api_key)

    return {
        "account_id": str(account_id),
        "api_key": api_key,
        "twilio_subaccount_sid": twilio_subaccount_sid
    }

class DeleteAccountPayload(BaseModel):
    account_id: uuid.UUID


# Renew API Key

@app.post("/v1/account/renew-api-key")
async def renew_api_key(account_id: uuid.UUID = Depends(get_account_from_api_key)):
    try:
        async with get_db_conn() as conn:
            new_api_key = generate_api_key()
            
            await conn.execute('''
                UPDATE accounts 
                SET api_key = $1, api_key_created_at = CURRENT_TIMESTAMP
                WHERE account_id = $2
            ''', new_api_key, account_id)
            
            return {
                "account_id": str(account_id),
                "api_key": new_api_key
            }
            
    except Exception as e:
        logger.error(f"Error renewing API key: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to renew API key")
    
    

# Delete Account

@app.delete("/v1/account")
async def delete_account(account_id: uuid.UUID = Depends(get_account_from_api_key)):
    async with get_db_conn() as conn:
        async with conn.transaction():
            # Get Twilio subaccount SID before deleting
            account = await conn.fetchrow(
                'SELECT twilio_subaccount_sid FROM accounts WHERE account_id = $1',
                account_id
            )
            
            if not account:
                raise HTTPException(status_code=404, detail="Account not found")

            # Delete Twilio subaccount
            try:
                if account['twilio_subaccount_sid']:
                    twilio_client.api.v2010.accounts(account['twilio_subaccount_sid']).update(
                        status='closed'
                    )
            except TwilioRestException as e:
                logger.error(f"Error closing Twilio subaccount: {str(e)}")

            # Delete all data in the correct order to respect foreign key constraints
            # 1. Delete contacts
            await conn.execute('DELETE FROM contacts WHERE account_id = $1', account_id)
            
            # 2. Delete contact lists
            await conn.execute('DELETE FROM contact_lists WHERE account_id = $1', account_id)
            
            # 3. Delete campaigns
            await conn.execute('DELETE FROM campaigns WHERE account_id = $1', account_id)
            
            # 4. Delete calls
            await conn.execute('DELETE FROM calls WHERE account_id = $1', account_id)
            
            # 5. Get agent IDs before deletion for cleanup
            agent_ids = await conn.fetch('SELECT agent_id FROM agents WHERE account_id = $1', account_id)
            
            # 6. Delete agents
            await conn.execute('DELETE FROM agents WHERE account_id = $1', account_id)
            
            # 7. Delete user record - Convert UUID to string
            await conn.execute('DELETE FROM "User" WHERE "accountId" = $1', str(account_id))
            
            # 8. Finally delete the account
            await conn.execute('DELETE FROM accounts WHERE account_id = $1', account_id)
            
            # Clean up Redis and file system
            for row in agent_ids:
                agent_id = str(row['agent_id'])
                await redis_client.delete(agent_id)
                
                stored_prompt_file_path = os.path.join(os.getcwd(), agent_id, "conversation_details.json")
                try:
                    os.remove(stored_prompt_file_path)
                    logger.info(f"Deleted file: {stored_prompt_file_path}")
                except FileNotFoundError:
                    logger.warning(f"File not found: {stored_prompt_file_path}")
                except Exception as e:
                    logger.error(f"Error deleting file {stored_prompt_file_path}: {str(e)}")

    return {"message": "Account and all associated data deleted successfully"}



# Get Account

@app.get("/v1/account")
async def get_account(account_id: uuid.UUID = Depends(get_account_from_api_key)):
    try:
        async with get_db_conn() as conn:
            account = await conn.fetchrow('''
                SELECT 
                    account_id, 
                    number_of_agents, 
                    total_calls, 
                    total_duration, 
                    total_cost,
                    created_at,
                    api_key_created_at
                FROM accounts 
                WHERE account_id = $1
            ''', account_id)
            
            if not account:
                logger.warning(f"Account not found: {account_id}")
                raise HTTPException(status_code=404, detail="Account not found")
            
            agents = await conn.fetch('SELECT agent_id, name FROM agents WHERE account_id = $1', account_id)
            
        agent_list = [{"agent_id": str(agent['agent_id']), "name": agent['name']} for agent in agents]
        
        return {
            "account_id": str(account['account_id']),
            "number_of_agents": account['number_of_agents'],
            "total_calls": account['total_calls'],
            "total_duration_minutes": round(account['total_duration'] / 60, 2),
            "total_cost": round(float(account['total_cost']), 2),
            "created_at": account['created_at'].isoformat(),
            "agents": agent_list
        }
    except Exception as e:
        logger.error(f"Error retrieving account {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve account details")


######################################
# Agent Management
######################################

class CreateAgentPayload(BaseModel):
    agent_config: AgentModel
    agent_prompts: Dict

@app.post("/v1/agent")
async def create_agent(
    agent_data: CreateAgentPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    agent_uuid = str(uuid.uuid4())
    data_for_db = agent_data.agent_config.model_dump()
    agent_prompts = agent_data.agent_prompts
    call_direction = data_for_db.get("call_direction")
    webhook_url = data_for_db.get("webhook_url")
    inbound_phone_number = data_for_db.get("inbound_phone_number")
    agent_image = data_for_db.get("agent_image")
    is_compliant = data_for_db.get("is_compliant", False)  # Get is_compliant with default False

    # Validate inbound_phone_number usage
    if call_direction == "outbound" and inbound_phone_number:
        raise HTTPException(
            status_code=400,
            detail="inbound_phone_number cannot be set for outbound agents"
        )

    async with get_db_conn() as conn:
        # Check if phone number is already configured for another agent
        if call_direction == "inbound" and inbound_phone_number:
            existing_agent = await conn.fetchrow('''
                SELECT name FROM agents 
                WHERE inbound_phone_number = $1 
                AND account_id = $2
                AND deleted_at IS NULL
            ''', inbound_phone_number, account_id)
            
            if existing_agent:
                raise HTTPException(
                    status_code=400,
                    detail=f"Phone number {inbound_phone_number} is already configured for agent '{existing_agent['name']}'"
                )

        async with conn.transaction():
            # Insert into agents table with consolidated JSON
            await conn.execute('''
                INSERT INTO agents (
                    agent_id, account_id, name, agent_type,
                    call_direction, inbound_phone_number,
                    timezone, country, agent_image,
                    webhook_url, agent_config, agent_prompts,
                    is_compliant
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ''', 
            uuid.UUID(agent_uuid),
            account_id,
            data_for_db.get("agent_name"),
            data_for_db.get("agent_type"),
            call_direction,
            inbound_phone_number,
            data_for_db.get("timezone", "America/Los_Angeles"),
            data_for_db.get("country", "US"),
            agent_image,
            webhook_url,
            json.dumps(data_for_db),  # Store full config as JSON
            json.dumps(agent_prompts),  # Store prompts as JSON
            is_compliant  # Add the is_compliant field
            )

            await conn.execute('UPDATE accounts SET number_of_agents = number_of_agents + 1 WHERE account_id = $1', account_id)

            # If it's an inbound agent with a phone number, configure the webhook
            if call_direction == "inbound" and inbound_phone_number:
                try:
                    # Get the kallabot URLs
                    app_callback_url, websocket_url = populate_custom_urls()

                    # Store the mapping in Redis
                    mapping_key = f"inbound_mapping:{inbound_phone_number}"
                    await redis_client.set(mapping_key, agent_uuid)

                    # Update Twilio phone number configuration
                    incoming_phone_number = twilio_client.incoming_phone_numbers.list(
                        phone_number=inbound_phone_number
                    )[0]
                    
                    incoming_phone_number.update(
                        voice_url=f"{app_callback_url}/inbound/webhook",
                        voice_method='POST'
                    )

                except IndexError:
                    await conn.execute('ROLLBACK')
                    raise HTTPException(
                        status_code=404, 
                        detail="Phone number not found in your Twilio account"
                    )
                except TwilioRestException as e:
                    await conn.execute('ROLLBACK')
                    raise HTTPException(
                        status_code=e.status, 
                        detail=f"Twilio API error: {str(e)}"
                    )

            # Store in Redis and file system
            await asyncio.gather(
                redis_client.set(agent_uuid, json.dumps(data_for_db)),
                store_file(file_key=f"{agent_uuid}/conversation_details.json", file_data=agent_prompts, local=True)
            )

            return {
                "agent_id": agent_uuid,
                "message": "Agent created successfully",
                "state": "created",
                "inbound_configured": call_direction == "inbound" and inbound_phone_number is not None
            }



# Delete Agent

class DeleteAgentPayload(BaseModel):
    account_id: uuid.UUID
    agent_id: uuid.UUID

@app.delete("/v1/agent/{agent_id}")
async def delete_agent(
    agent_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            async with conn.transaction():
                # Soft delete the agent instead of hard delete
                result = await conn.fetchrow('''
                    UPDATE agents 
                    SET deleted_at = CURRENT_TIMESTAMP
                    WHERE agent_id = $1 AND account_id = $2 AND deleted_at IS NULL
                    RETURNING inbound_phone_number
                ''', agent_id, account_id)
                
                if not result:
                    raise HTTPException(
                        status_code=404,
                        detail="Agent not found or doesn't belong to the account"
                    )
                
                # Update account's agent count
                await conn.execute('''
                    UPDATE accounts 
                    SET number_of_agents = number_of_agents - 1 
                    WHERE account_id = $1
                ''', account_id)
                
                # Clean up Redis and file system
                await redis_client.delete(str(agent_id))
                
                stored_prompt_file_path = os.path.join(
                    os.getcwd(), 
                    str(agent_id), 
                    "conversation_details.json"
                )
                try:
                    os.remove(stored_prompt_file_path)
                except FileNotFoundError:
                    pass
                
                if result['inbound_phone_number']:
                    mapping_key = f"inbound_mapping:{result['inbound_phone_number']}"
                    await redis_client.delete(mapping_key)
                
                return {
                    "status": "success",
                    "message": "Agent deleted successfully",
                    "agent_id": str(agent_id)
                }
                
    except Exception as e:
        logger.error(f"Error deleting agent: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete agent")


# Get All Agents

@app.get("/v1/agents")
async def get_agents(account_id: uuid.UUID = Depends(get_account_from_api_key)):
    try:
        async with get_db_conn() as conn:
            # First, let's check if the column exists
            check_column = await conn.fetchrow('''
                SELECT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'agents' 
                    AND column_name = 'agent_image'
                );
            ''')
            
            if not check_column['exists']:
                # If column doesn't exist, add it
                await conn.execute('''
                    ALTER TABLE agents ADD COLUMN IF NOT EXISTS agent_image TEXT;
                ''')

            # Now fetch the agents
            agents = await conn.fetch('''
                SELECT 
                    agent_id, name, agent_type, call_direction,
                    inbound_phone_number, total_calls, total_duration,
                    total_cost, created_at, timezone, country,
                    COALESCE(agent_image, '') as agent_image,
                    webhook_url, is_compliant,
                    agent_config, agent_prompts
                FROM agents 
                WHERE account_id = $1 
                AND deleted_at IS NULL
                ORDER BY created_at DESC
            ''', account_id)
            
            return {
                "agents": [
                    {
                        "agent_id": str(agent['agent_id']),
                        "name": agent['name'],
                        "agent_type": agent['agent_type'],
                        "call_direction": agent['call_direction'],
                        "inbound_phone_number": agent['inbound_phone_number'],
                        "agent_image": agent['agent_image'] or None,
                        "webhook_url": agent['webhook_url'],
                        "is_compliant": agent['is_compliant'],
                        "total_calls": agent['total_calls'],
                        "total_duration": agent['total_duration'],
                        "total_cost": float(agent['total_cost']),
                        "created_at": agent['created_at'].isoformat(),
                        "timezone": agent['timezone'],
                        "country": agent['country'],
                        "agent_config": json.loads(agent['agent_config']),
                        "agent_prompts": json.loads(agent['agent_prompts'])
                    }
                    for agent in agents
                ]
            }
            
    except Exception as e:
        logger.error(f"Error fetching agents: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch agents: {str(e)}")



# Get Agent
@app.get("/v1/agent/{agent_id}")
async def get_agent(
    agent_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            agent = await conn.fetchrow('''
                SELECT 
                    a.*,
                    k.kb_id,
                    k.vector_store_id,
                    k.friendly_name as kb_name,
                    k.status as kb_status
                FROM agents a
                LEFT JOIN knowledgebases k ON 
                    k.vector_store_id = (a.agent_config->'tools_config'->'api_tools'->'provider_config'->>'vector_id')
                WHERE a.agent_id = $1 
                AND a.account_id = $2
                AND a.deleted_at IS NULL
            ''', agent_id, account_id)
            
            if not agent:
                raise HTTPException(
                    status_code=404,
                    detail="Agent not found or doesn't belong to the account"
                )
            
            # Parse JSON fields
            agent_config = json.loads(agent['agent_config'])
            agent_prompts = json.loads(agent['agent_prompts'])
            
            # Build complete response with full configuration
            response = {
                # Basic Info
                "agent_id": str(agent['agent_id']),
                "account_id": str(agent['account_id']),
                "name": agent['name'],
                "agent_type": agent['agent_type'],
                "call_direction": agent['call_direction'],
                "inbound_phone_number": agent['inbound_phone_number'],
                "agent_image": agent['agent_image'],
                "webhook_url": agent['webhook_url'],
                "total_calls": agent['total_calls'],
                "total_duration": agent['total_duration'],
                "total_cost": float(agent['total_cost']),
                "created_at": agent['created_at'].isoformat(),
                "timezone": agent['timezone'],
                "country": agent['country'],
                "template_variables": agent['template_variables'],
                "is_compliant": agent['is_compliant'],
                "agent_config": agent_config,
                "agent_prompts": agent_prompts,
                "knowledgebase": {
                    "kb_id": str(agent['kb_id']) if agent['kb_id'] else None,
                    "vector_store_id": agent['vector_store_id'],
                    "name": agent['kb_name'],
                    "status": agent['kb_status']
                } if agent['kb_id'] else None
            }
            
            return response
            
    except Exception as e:
        logger.error(f"Error retrieving agent: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve agent details")


# Update Agent

class UpdateAgentPayload(BaseModel):
    agent_id: uuid.UUID
    agent_config: Dict[str, Any]
    agent_prompts: Dict[str, Any]

@app.put("/v1/agent/{agent_id}")
async def update_agent(
    agent_id: uuid.UUID,
    payload: UpdateAgentPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            agent = await conn.fetchrow('''
                SELECT 1 FROM agents 
                WHERE agent_id = $1 AND account_id = $2
            ''', agent_id, account_id)
            
            if not agent:
                raise HTTPException(
                    status_code=404,
                    detail="Agent not found or doesn't belong to the account"
                )
            
            await conn.execute('''
                UPDATE agents 
                SET agent_config = $1, agent_prompts = $2
                WHERE agent_id = $3 AND account_id = $4
            ''', json.dumps(payload.agent_config), json.dumps(payload.agent_prompts),
                agent_id, account_id)
            
            await redis_client.set(str(agent_id), json.dumps(payload.agent_config))
            
            await store_file(
                file_key=f"{agent_id}/conversation_details.json",
                file_data=payload.agent_prompts,
                local=True
            )
            
            return {
                "status": "success",
                "message": "Agent updated successfully",
                "agent_id": str(agent_id)
            }
    except Exception as e:
        logger.error(f"Error updating agent: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to update agent")


# Websocket

@app.websocket("/v1/chat/{agent_id}")
async def websocket_endpoint(agent_id: str, websocket: WebSocket, user_agent: str = Query(None)):
    logger.info("Connected to ws")
    await websocket.accept()
    active_websockets.append(websocket)
    
    try:
        async with get_db_conn() as conn:
            agent = await conn.fetchrow('''
                SELECT agent_config, template_variables
                FROM agents 
                WHERE agent_id = $1
            ''', uuid.UUID(agent_id))
            
            if not agent:
                raise HTTPException(status_code=404, detail="Agent not found")
            
            agent_config = json.loads(agent['agent_config'])
            # Merge template variables if they exist
            if agent['template_variables']:
                agent_config['template_variables'] = agent['template_variables']

        assistant_manager = AssistantManager(agent_config, websocket, agent_id)
        async for index, task_output in assistant_manager.run(local=True):
            logger.info(task_output)
            
    except WebSocketDisconnect:
        active_websockets.remove(websocket)
    except Exception as e:
        logger.error(f"Error in executing: {str(e)}")
        traceback.print_exc()
    finally:
        if websocket in active_websockets:
            active_websockets.remove(websocket)




#####################################################################
# Knowledgebase 
#####################################################################

class KnowledgebaseConfig(BaseModel):
    table:str = "None"
    chunk_size:int = 512
    overlapping:int = 200

# Create Knowledgebase

@app.post("/v1/knowledgebase")
async def create_knowledgebase(
    friendly_name: str = Form(...),
    file: UploadFile = File(...),
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        # Validate file type
        if file.content_type not in ["application/pdf", "application/x-pdf"]:
            raise HTTPException(status_code=400, detail="Only PDF files are accepted")

        async with get_db_conn() as conn:
            # Check for duplicate name
            existing_kb = await conn.fetchrow(
                'SELECT kb_id FROM knowledgebases WHERE account_id = $1 AND friendly_name = $2',
                account_id, friendly_name
            )
            if existing_kb:
                raise HTTPException(status_code=400, detail="Friendly name already exists")

            # Create new knowledge base record
            kb_id = uuid.uuid4()
            vector_store_id = str(uuid.uuid4())
            
            # Save file temporarily
            temp_file = tempfile.NamedTemporaryFile(delete=False)
            file_content = await file.read()
            temp_file.write(file_content)
            temp_file.close()

            # Process file in background
            file_name = f"/tmp/{vector_store_id}.pdf"
            os.rename(temp_file.name, file_name)
            
            # Insert record
            await conn.execute('''
                INSERT INTO knowledgebases (
                    kb_id, account_id, friendly_name, vector_store_id, 
                    file_name, file_type, size_bytes, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', kb_id, account_id, friendly_name, vector_store_id,
                file.filename, file.content_type, len(file_content), 'processed')

            # Process in background
            thread = threading.Thread(
                target=create_table,
                args=(vector_store_id, file_name)
            )
            thread.start()

            return {
                "kb_id": str(kb_id),
                "vector_store_id": vector_store_id,
                "status": "processed",
                "friendly_name": friendly_name
            }

    except Exception as e:
        logger.error(f"Error creating knowledge base: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create knowledge base")



# Get all Knowledgebases

@app.get("/v1/knowledgebases")
async def get_knowledgebases(
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            kbs = await conn.fetch('''
                SELECT kb_id, friendly_name, vector_store_id, status, created_at
                FROM knowledgebases 
                WHERE account_id = $1
                ORDER BY created_at DESC
            ''', account_id)
            
            return {
                "knowledgebases": [
                    {
                        "kb_id": str(kb['kb_id']),
                        "friendly_name": kb['friendly_name'],
                        "vector_store_id": kb['vector_store_id'],
                        "status": kb['status'],
                        "created_at": kb['created_at'].isoformat()
                    }
                    for kb in kbs
                ]
            }
    except Exception as e:
        logger.error(f"Error fetching knowledge bases: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch knowledge bases")


# Delete Knowledgebase

@app.delete("/v1/knowledgebase/{kb_id}")
async def delete_knowledgebase(
    kb_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            # Verify ownership and get vector_store_id
            kb = await conn.fetchrow('''
                SELECT vector_store_id, file_name 
                FROM knowledgebases 
                WHERE kb_id = $1 AND account_id = $2
            ''', kb_id, account_id)
            
            if not kb:
                raise HTTPException(
                    status_code=404,
                    detail="Knowledge base not found or doesn't belong to the account"
                )
            
            # Delete vector store files
            vector_store_path = os.path.join('local_setup', 'RAG', f"{kb['vector_store_id']}.lance")
            if os.path.exists(vector_store_path):
                try:
                    import shutil
                    shutil.rmtree(vector_store_path)
                except Exception as e:
                    logger.error(f"Error deleting vector store files: {str(e)}")
            
            # Delete from database
            await conn.execute(
                'DELETE FROM knowledgebases WHERE kb_id = $1 AND account_id = $2',
                kb_id, account_id
            )
            
            return {
                "status": "success",
                "message": "Knowledge base deleted successfully",
                "kb_id": str(kb_id)
            }
            
    except Exception as e:
        logger.error(f"Error deleting knowledge base: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete knowledge base")

#####################################################################
# Analytics
#####################################################################

# Get Details
@app.get("/v1/details")
async def get_details(account_id: uuid.UUID = Depends(get_account_from_api_key)):
    try:
        async with get_db_conn() as conn:
            calls = await conn.fetch('''
                SELECT 
                    c.call_sid,
                    c.agent_id,
                    c.account_id,
                    c.from_number,
                    c.to_number,
                    c.duration,
                    c.recording_url,
                    c.transcription,
                    c.status,
                    c.call_type,
                    c.cost,
                    c.created_at,
                    a.name
                FROM calls c
                JOIN agents a ON c.agent_id = a.agent_id
                WHERE c.account_id = $1
                ORDER BY c.created_at DESC
            ''', account_id)
            
        if not calls:
            logger.warning(f"No calls found for account: {account_id}")
            return {"calls": []}
        
        call_details = [
            {
                "call_sid": str(call['call_sid']),
                "agent_id": str(call['agent_id']),
                "name": call['name'],
                "account_id": str(call['account_id']),
                "from_number": call['from_number'],
                "to_number": call['to_number'],
                "duration": call['duration'],
                "recording_url": call['recording_url'],
                "transcription": call['transcription'],
                "status": call['status'],
                "cost": call['cost'],
                "call_type": call['call_type'],
                "created_at": call['created_at'].isoformat()
            }
            for call in calls
        ]
        
        return {"calls": call_details}
    except Exception as e:
        logger.error(f"Error retrieving call details for account {account_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Get Call Details

@app.get("/v1/call-details/{call_sid}")
async def get_call_details(
    call_sid: str,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            call = await conn.fetchrow('''
                SELECT 
                    call_sid,
                    agent_id,
                    account_id,
                    from_number,
                    to_number,
                    duration,
                    recording_url,
                    transcription,
                    status,
                    call_type,
                    cost,
                    created_at
                FROM calls 
                WHERE call_sid = $1 AND account_id = $2
            ''', call_sid, account_id)
            
        if not call:
            logger.warning(f"Call not found: {call_sid}")
            raise HTTPException(status_code=404, detail="Call not found")
        
        call_details = {
            "call_sid": call['call_sid'],
            "agent_id": str(call['agent_id']),
            "account_id": str(call['account_id']),
            "from_number": call['from_number'],
            "to_number": call['to_number'],
            "duration": call['duration'],
            "recording_url": call['recording_url'],
            "transcription": call['transcription'],
            "status": call['status'],
            "call_type": call['call_type'],
            "cost": call['cost'],
            "created_at": call['created_at'].isoformat()
        }
        
        return call_details
    except Exception as e:
        logger.error(f"Error retrieving call details for call SID {call_sid}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")


def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

scheduler = AsyncIOScheduler()
scheduler.add_jobstore(RedisJobStore(host='redis', port=6379))






#####################################################################
# Contacts
#####################################################################

class ContactModel(BaseModel):
    phone_number: str
    template_variables: Dict[str, Any]

class CreateContactListPayload(BaseModel):
    name: str
    description: str
    contacts: List[ContactModel]

class GetContactsPayload(BaseModel):
    account_id: uuid.UUID
    list_id: uuid.UUID

class EditContactListPayload(BaseModel):
    contacts: List[ContactModel]

class GetContactsRequest(BaseModel):
    list_id: uuid.UUID
    account_id: uuid.UUID

# Create Contact List

@app.post("/v1/contacts")
async def create_contact_list(
    payload: CreateContactListPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            async with conn.transaction():
                list_id = uuid.uuid4()
                await conn.execute('''
                    INSERT INTO contact_lists (list_id, account_id, name, description)
                    VALUES ($1, $2, $3, $4)
                ''', list_id, account_id, payload.name, payload.description)

                for contact in payload.contacts:
                    contact_id = uuid.uuid4()
                    await conn.execute('''
                        INSERT INTO contacts (contact_id, list_id, account_id, phone_number, template_variables)
                        VALUES ($1, $2, $3, $4, $5)
                    ''', contact_id, list_id, account_id, contact.phone_number, json.dumps(contact.template_variables))

        return {
            "list_id": str(list_id),
            "status": "created",
            "contact_count": len(payload.contacts)
        }
    except Exception as e:
        logger.error(f"Error creating contact list: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to create contact list")

# Get Contact Lists
@app.get("/v1/contacts/lists")
async def get_contact_lists(
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            lists = await conn.fetch('''
                SELECT list_id, name, description, created_at
                FROM contact_lists 
                WHERE account_id = $1
                ORDER BY created_at DESC
            ''', account_id)
            
        return {
            "contact_lists": [
                {
                    "list_id": str(list['list_id']),
                    "name": list['name'],
                    "description": list['description'],
                    "created_at": list['created_at'].isoformat()
                }
                for list in lists
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching contact lists: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch contact lists")

# Get all Contacts
@app.get("/v1/contacts")
async def get_contacts(
    list_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            contacts = await conn.fetch('''
                SELECT contact_id, phone_number, template_variables, created_at
                FROM contacts 
                WHERE list_id = $1 AND account_id = $2
                ORDER BY created_at DESC
            ''', list_id, account_id)
            
        return {
            "contacts": [
                {
                    "contact_id": str(contact['contact_id']),
                    "phone_number": contact['phone_number'],
                    "template_variables": contact['template_variables'],
                    "created_at": contact['created_at'].isoformat()
                }
                for contact in contacts
            ]
        }
    except Exception as e:
        logger.error(f"Error fetching contacts: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch contacts")

# Edit Contact List

@app.put("/v1/contacts/{list_id}")
async def update_contacts(
    list_id: uuid.UUID,
    payload: EditContactListPayload,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    pool = await get_db_pool()
    try:
        async with pool.acquire() as conn:
            contact_list = await conn.fetchrow(
                'SELECT list_id FROM contact_lists WHERE list_id = $1 AND account_id = $2',
                list_id, account_id
            )
            
            if not contact_list:
                raise HTTPException(
                    status_code=404,
                    detail="Contact list not found or doesn't belong to the account"
                )
            
            async with conn.transaction():
                await conn.execute('''
                    DELETE FROM contacts 
                    WHERE list_id = $1 AND account_id = $2
                ''', list_id, account_id)
                
                added_contacts = []
                for contact in payload.contacts:
                    contact_id = uuid.uuid4()
                    await conn.execute('''
                        INSERT INTO contacts (
                            contact_id, list_id, account_id, 
                            phone_number, template_variables
                        )
                        VALUES ($1, $2, $3, $4, $5)
                    ''', contact_id, list_id, account_id, 
                        contact.phone_number, json.dumps(contact.template_variables)
                    )
                    added_contacts.append(str(contact_id))
                
                return {
                    "status": "success",
                    "message": "Contact list updated successfully",
                    "list_id": str(list_id),
                    "total_contacts": len(added_contacts),
                    "contact_ids": added_contacts
                }
                
    except Exception as e:
        logger.error(f"Error updating contact list: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to update contact list")
    


# Delete Contact List
@app.delete("/v1/contacts/{list_id}")
async def delete_contact_list(
    list_id: uuid.UUID,
    account_id: uuid.UUID = Depends(get_account_from_api_key)
):
    try:
        async with get_db_conn() as conn:
            async with conn.transaction():
                contact_list = await conn.fetchrow(
                    'SELECT list_id FROM contact_lists WHERE list_id = $1 AND account_id = $2',
                    list_id, account_id
                )
                
                if not contact_list:
                    raise HTTPException(
                        status_code=404,
                        detail="Contact list not found or doesn't belong to the account"
                    )

                campaign = await conn.fetchrow(
                    'SELECT campaign_id FROM campaigns WHERE list_id = $1',
                    list_id
                )
                
                if campaign:
                    raise HTTPException(
                        status_code=400,
                        detail="Cannot delete list as it is being used in one or more campaigns"
                    )
                
                await conn.execute(
                    'DELETE FROM contacts WHERE list_id = $1 AND account_id = $2',
                    list_id, account_id
                )
                
                await conn.execute(
                    'DELETE FROM contact_lists WHERE list_id = $1 AND account_id = $2',
                    list_id, account_id
                )
                
        return {"status": "deleted"}
    except Exception as e:
        logger.error(f"Error deleting contact list: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to delete contact list")

async def get_connection_stats():
    async with get_db_conn() as conn:
        result = await conn.fetch("""
            SELECT count(*) as active_connections
            FROM pg_stat_activity
            WHERE state = 'active'
        """)
        return result[0]['active_connections']

@app.get("/v1/debug/connection-stats")
async def connection_stats():
    active_connections = await get_connection_stats()
    return {
        "active_connections": active_connections,
        "pool_size": _pool.get_size() if _pool else 0,
        "pool_idle_size": _pool.get_idle_size() if _pool else 0
    }