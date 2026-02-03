"""
Device Management API Controller.
Handles all HTTP endpoints for device management.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List

from helpers.config import session_factory, logger
from dal.device_dao import DeviceDAO
from entities.device import Device, DeviceStatus, DeviceType
from dto.device_dto import (
    DeviceRequest,
    DeviceResponse,
    DeviceListResponse,
    DeviceUpdateRequest,
    DeviceStatusUpdateRequest,
    DeployRequest,
    RecallRequest,
    MaintenanceRequest,
    ActionResponse,
    DeviceStatusDto,
    DeviceTypeDto,
    ReserveRequest,
    TelemetryRequest,
    DeviceEventRequest
)
from services.jwt_service import get_current_user, AuthUser
from services.rabbitmq_publisher import RabbitMQPublisher

router = APIRouter(prefix="/devices", tags=["devices"])
rabbitmq_publisher = RabbitMQPublisher()


def get_db_session():
    """Dependency to get database session."""
    return next(session_factory())


@router.get("/", response_model=DeviceListResponse)
def get_all_devices(
    status: Optional[DeviceStatusDto] = Query(None, description="Filter by status"),
    device_type: Optional[DeviceTypeDto] = Query(None, description="Filter by type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Get all devices with optional filtering and pagination.
    
    Returns a paginated list of devices.
    """
    logger.info(f"User {current_user.email} getting devices - status: {status}, type: {device_type}, page: {page}")
    
    devices, total = DeviceDAO.get_all_devices(
        session=session,
        status=status,
        device_type=device_type,
        page=page,
        page_size=page_size
    )
    
    device_responses = [
        DeviceResponse(
            id=d.id,
            name=d.name,
            type=d.type.value if d.type else None,
            serial_number=d.serial_number,
            description=d.description,
            status=d.status.value if d.status else None,
            location=d.location,
            specifications=d.specifications,
            purchase_date=str(d.purchase_date) if d.purchase_date else None,
            deploy_date=str(d.deploy_date) if d.deploy_date else None,
            last_maintenance_date=str(d.last_maintenance_date) if d.last_maintenance_date else None,
            created_at=str(d.created_at) if d.created_at else None,
            updated_at=str(d.updated_at) if d.updated_at else None
        )
        for d in devices
    ]
    
    return DeviceListResponse(
        devices=device_responses,
        total=total,
        page=page,
        page_size=page_size
    )


@router.get("/in_stock", response_model=List[DeviceResponse])
def get_in_stock_devices(
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Get all devices that are in stock (available).
    """
    logger.info(f"User {current_user.email} getting in-stock devices")
    
    devices = DeviceDAO.get_devices_by_status(session, DeviceStatusDto.IN_STOCK)
    
    return [
        DeviceResponse(
            id=d.id,
            name=d.name,
            type=d.type.value if d.type else None,
            serial_number=d.serial_number,
            description=d.description,
            status=d.status.value if d.status else None,
            location=d.location,
            specifications=d.specifications,
            purchase_date=str(d.purchase_date) if d.purchase_date else None,
            deploy_date=str(d.deploy_date) if d.deploy_date else None,
            last_maintenance_date=str(d.last_maintenance_date) if d.last_maintenance_date else None,
            created_at=str(d.created_at) if d.created_at else None,
            updated_at=str(d.updated_at) if d.updated_at else None
        )
        for d in devices
    ]


@router.get("/deployed", response_model=List[DeviceResponse])
def get_deployed_devices(
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Get all devices that are deployed in the field.
    """
    logger.info(f"User {current_user.email} getting deployed devices")
    
    devices = DeviceDAO.get_devices_by_status(session, DeviceStatusDto.DEPLOYED)
    
    return [
        DeviceResponse(
            id=d.id,
            name=d.name,
            type=d.type.value if d.type else None,
            serial_number=d.serial_number,
            description=d.description,
            status=d.status.value if d.status else None,
            location=d.location,
            specifications=d.specifications,
            purchase_date=str(d.purchase_date) if d.purchase_date else None,
            deploy_date=str(d.deploy_date) if d.deploy_date else None,
            last_maintenance_date=str(d.last_maintenance_date) if d.last_maintenance_date else None,
            created_at=str(d.created_at) if d.created_at else None,
            updated_at=str(d.updated_at) if d.updated_at else None
        )
        for d in devices
    ]


@router.get("/maintenance", response_model=List[DeviceResponse])
def get_maintenance_devices(
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Get all devices that are in maintenance.
    """
    logger.info(f"User {current_user.email} getting maintenance devices")
    
    devices = DeviceDAO.get_devices_by_status(session, DeviceStatusDto.MAINTENANCE)
    
    return [
        DeviceResponse(
            id=d.id,
            name=d.name,
            type=d.type.value if d.type else None,
            serial_number=d.serial_number,
            description=d.description,
            status=d.status.value if d.status else None,
            location=d.location,
            specifications=d.specifications,
            purchase_date=str(d.purchase_date) if d.purchase_date else None,
            deploy_date=str(d.deploy_date) if d.deploy_date else None,
            last_maintenance_date=str(d.last_maintenance_date) if d.last_maintenance_date else None,
            created_at=str(d.created_at) if d.created_at else None,
            updated_at=str(d.updated_at) if d.updated_at else None
        )
        for d in devices
    ]


@router.get("/{device_id}", response_model=DeviceResponse)
def get_device_by_id(
    device_id: int, 
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Get a specific device by its ID.
    """
    logger.info(f"User {current_user.email} getting device with ID: {device_id}")
    
    device = DeviceDAO.get_device_by_id(session, device_id)
    if not device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    return DeviceResponse(
        id=device.id,
        name=device.name,
        type=device.type.value if device.type else None,
        serial_number=device.serial_number,
        description=device.description,
        status=device.status.value if device.status else None,
        location=device.location,
        specifications=device.specifications,
        purchase_date=str(device.purchase_date) if device.purchase_date else None,
        deploy_date=str(device.deploy_date) if device.deploy_date else None,
        last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
        created_at=str(device.created_at) if device.created_at else None,
        updated_at=str(device.updated_at) if device.updated_at else None
    )


@router.post("/", response_model=DeviceResponse, status_code=201)
def create_device(
    device_request: DeviceRequest, 
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Register a new device in the system.
    """
    logger.info(f"User {current_user.email} creating new device: {device_request.name}")
    
    # Convert DTO to entity
    device = Device(
        name=device_request.name,
        type=DeviceType(device_request.type.value),
        serial_number=device_request.serial_number,
        description=device_request.description,
        location=device_request.location,
        specifications=device_request.specifications
    )
    
    # Create in database
    success = DeviceDAO.create_device(session, device)
    if not success:
        raise HTTPException(status_code=400, detail="Device with this serial number already exists")
    
    logger.info(f"Device created successfully with ID: {device.id}")
    
    return DeviceResponse(
        id=device.id,
        name=device.name,
        type=device.type.value if device.type else None,
        serial_number=device.serial_number,
        description=device.description,
        status=device.status.value if device.status else None,
        location=device.location,
        specifications=device.specifications,
        purchase_date=str(device.purchase_date) if device.purchase_date else None,
        deploy_date=str(device.deploy_date) if device.deploy_date else None,
        last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
        created_at=str(device.created_at) if device.created_at else None,
        updated_at=str(device.updated_at) if device.updated_at else None
    )


@router.put("/{device_id}", response_model=DeviceResponse)
def update_device(
    device_id: int, 
    update_request: DeviceUpdateRequest,
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Update device information.
    """
    logger.info(f"User {current_user.email} updating device: {device_id}")
    
    device = DeviceDAO.update_device(
        session=session,
        device_id=device_id,
        name=update_request.name,
        description=update_request.description,
        location=update_request.location,
        specifications=update_request.specifications
    )
    
    if not device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    logger.info(f"Device {device_id} updated successfully")
    
    return DeviceResponse(
        id=device.id,
        name=device.name,
        type=device.type.value if device.type else None,
        serial_number=device.serial_number,
        description=device.description,
        status=device.status.value if device.status else None,
        location=device.location,
        specifications=device.specifications,
        purchase_date=str(device.purchase_date) if device.purchase_date else None,
        deploy_date=str(device.deploy_date) if device.deploy_date else None,
        last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
        created_at=str(device.created_at) if device.created_at else None,
        updated_at=str(device.updated_at) if device.updated_at else None
    )


@router.delete("/{device_id}", response_model=ActionResponse)
def delete_device(
    device_id: int, 
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Delete (retire) a device.
    """
    logger.info(f"User {current_user.email} deleting (retiring) device: {device_id}")
    
    success = DeviceDAO.delete_device(session, device_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    logger.info(f"Device {device_id} retired successfully")
    
    return ActionResponse(
        success=True,
        message=f"Device {device_id} has been retired"
    )


# ===================== STATUS MANAGEMENT ENDPOINTS =====================


@router.put("/{device_id}/status", response_model=DeviceResponse)
def update_device_status(
    device_id: int,
    status_request: DeviceStatusUpdateRequest,
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Update device status.
    """
    logger.info(f"User {current_user.email} updating device {device_id} status to: {status_request.status}")
    
    old_device = DeviceDAO.get_device_by_id(session, device_id)
    if not old_device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    old_status = old_device.status.value if old_device.status else None
    
    device = DeviceDAO.update_device_status(
        session=session,
        device_id=device_id,
        new_status=status_request.status,
        location=status_request.location
    )
    
    if not device:
        raise HTTPException(status_code=400, detail="Failed to update device status")
    
    # Log the action
    DeviceDAO.log_action(
        session=session,
        device_id=device_id,
        action="status_change",
        old_status=old_status,
        new_status=status_request.status.value,
        notes=status_request.notes
    )
    
    logger.info(f"Device {device_id} status updated to {status_request.status}")
    
    return DeviceResponse(
        id=device.id,
        name=device.name,
        type=device.type.value if device.type else None,
        serial_number=device.serial_number,
        description=device.description,
        status=device.status.value if device.status else None,
        location=device.location,
        specifications=device.specifications,
        purchase_date=str(device.purchase_date) if device.purchase_date else None,
        deploy_date=str(device.deploy_date) if device.deploy_date else None,
        last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
        created_at=str(device.created_at) if device.created_at else None,
        updated_at=str(device.updated_at) if device.updated_at else None
    )


@router.put("/{device_id}/deploy", response_model=ActionResponse)
def deploy_device(
    device_id: int,
    deploy_request: DeployRequest,
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Deploy a device to the field.
    """
    logger.info(f"User {current_user.email} deploying device {device_id} to: {deploy_request.location}")
    
    old_device = DeviceDAO.get_device_by_id(session, device_id)
    if not old_device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    old_status = old_device.status.value if old_device.status else None
    
    device = DeviceDAO.deploy_device(
        session=session,
        device_id=device_id,
        deployment_location=deploy_request.location
    )
    
    if not device:
        raise HTTPException(
            status_code=400, 
            detail="Device cannot be deployed. It must be in 'in_stock', 'reserved', or 'maintenance' status."
        )
    
    # Log the action
    DeviceDAO.log_action(
        session=session,
        device_id=device_id,
        action="deployed",
        old_status=old_status,
        new_status="deployed",
        notes=deploy_request.notes
    )
    
    logger.info(f"Device {device_id} deployed successfully")
    
    return ActionResponse(
        success=True,
        message=f"Device {device_id} deployed to {deploy_request.location}",
        device=DeviceResponse(
            id=device.id,
            name=device.name,
            type=device.type.value if device.type else None,
            serial_number=device.serial_number,
            description=device.description,
            status=device.status.value if device.status else None,
            location=device.location,
            specifications=device.specifications,
            purchase_date=str(device.purchase_date) if device.purchase_date else None,
            deploy_date=str(device.deploy_date) if device.deploy_date else None,
            last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
            created_at=str(device.created_at) if device.created_at else None,
            updated_at=str(device.updated_at) if device.updated_at else None
        )
    )


@router.put("/{device_id}/recall", response_model=ActionResponse)
def recall_device(
    device_id: int,
    recall_request: RecallRequest,
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Recall a device from the field back to stock.
    """
    logger.info(f"User {current_user.email} recalling device {device_id}")
    
    old_device = DeviceDAO.get_device_by_id(session, device_id)
    if not old_device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    old_status = old_device.status.value if old_device.status else None
    
    device = DeviceDAO.recall_device(
        session=session,
        device_id=device_id,
        warehouse_location=recall_request.location
    )
    
    if not device:
        raise HTTPException(
            status_code=400, 
            detail="Device cannot be recalled. It must be in 'deployed' status."
        )
    
    # Log the action
    DeviceDAO.log_action(
        session=session,
        device_id=device_id,
        action="recalled",
        old_status=old_status,
        new_status="in_stock",
        notes=recall_request.notes
    )
    
    logger.info(f"Device {device_id} recalled successfully")
    
    return ActionResponse(
        success=True,
        message=f"Device {device_id} recalled to stock",
        device=DeviceResponse(
            id=device.id,
            name=device.name,
            type=device.type.value if device.type else None,
            serial_number=device.serial_number,
            description=device.description,
            status=device.status.value if device.status else None,
            location=device.location,
            specifications=device.specifications,
            purchase_date=str(device.purchase_date) if device.purchase_date else None,
            deploy_date=str(device.deploy_date) if device.deploy_date else None,
            last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
            created_at=str(device.created_at) if device.created_at else None,
            updated_at=str(device.updated_at) if device.updated_at else None
        )
    )


@router.put("/{device_id}/maintenance", response_model=ActionResponse)
def send_to_maintenance(
    device_id: int,
    maintenance_request: MaintenanceRequest,
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Send a device to maintenance.
    """
    logger.info(f"User {current_user.email} sending device {device_id} to maintenance")
    
    old_device = DeviceDAO.get_device_by_id(session, device_id)
    if not old_device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    old_status = old_device.status.value if old_device.status else None
    
    device = DeviceDAO.send_to_maintenance(session, device_id)
    
    if not device:
        raise HTTPException(
            status_code=400, 
            detail="Device cannot be sent to maintenance."
        )
    
    # Log the action
    DeviceDAO.log_action(
        session=session,
        device_id=device_id,
        action="maintenance",
        old_status=old_status,
        new_status="maintenance",
        notes=maintenance_request.notes
    )
    
    logger.info(f"Device {device_id} sent to maintenance")
    
    return ActionResponse(
        success=True,
        message=f"Device {device_id} sent to maintenance",
        device=DeviceResponse(
            id=device.id,
            name=device.name,
            type=device.type.value if device.type else None,
            serial_number=device.serial_number,
            description=device.description,
            status=device.status.value if device.status else None,
            location=device.location,
            specifications=device.specifications,
            purchase_date=str(device.purchase_date) if device.purchase_date else None,
            deploy_date=str(device.deploy_date) if device.deploy_date else None,
            last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
            created_at=str(device.created_at) if device.created_at else None,
            updated_at=str(device.updated_at) if device.updated_at else None
        )
    )


@router.put("/{device_id}/reserve", response_model=ActionResponse)
def reserve_device(
    device_id: int,
    reserve_request: ReserveRequest,
    session: Session = Depends(get_db_session),
    current_user: AuthUser = Depends(get_current_user)
):
    """
    Reserve a device for an order.
    """
    logger.info(f"User {current_user.email} reserving device {device_id}")
    
    old_device = DeviceDAO.get_device_by_id(session, device_id)
    if not old_device:
        raise HTTPException(status_code=404, detail=f"Device with ID {device_id} not found")
    
    old_status = old_device.status.value if old_device.status else None
    
    device = DeviceDAO.reserve_device(session, device_id)
    
    if not device:
        raise HTTPException(
            status_code=400, 
            detail="Device cannot be reserved. It must be in 'in_stock' status."
        )
    
    # Log the action
    DeviceDAO.log_action(
        session=session,
        device_id=device_id,
        action="reserved",
        old_status=old_status,
        new_status="reserved",
        notes=reserve_request.notes
    )
    
    # Notify Monitoring Service via RabbitMQ
    rabbitmq_publisher.publish_device_event(
        device_id=device.id,
        device_name=device.name,
        event_type="device_reserved",
        details={"notes": reserve_request.notes}
    )
    
    logger.info(f"Device {device_id} reserved successfully")
    
    return ActionResponse(
        success=True,
        message=f"Device {device_id} reserved",
        device=DeviceResponse(
            id=device.id,
            name=device.name,
            type=device.type.value if device.type else None,
            serial_number=device.serial_number,
            description=device.description,
            status=device.status.value if device.status else None,
            location=device.location,
            specifications=device.specifications,
            purchase_date=str(device.purchase_date) if device.purchase_date else None,
            deploy_date=str(device.deploy_date) if device.deploy_date else None,
            last_maintenance_date=str(device.last_maintenance_date) if device.last_maintenance_date else None,
            created_at=str(device.created_at) if device.created_at else None,
            updated_at=str(device.updated_at) if device.updated_at else None
        )
    )


@router.post("/telemetry", response_model=ActionResponse)
def receive_telemetry(
    request: TelemetryRequest,
    session: Session = Depends(get_db_session)
):
    """
    Receive telemetry from a device and publish to RabbitMQ.
    Note: Usually telemetry doesn't require JWT for performance, 
    but you can add get_current_user dependecy if needed.
    """
    # Verify device exists
    device = DeviceDAO.get_device_by_id(session, request.device_id)
    if not device:
        # For security/privacy, we might just return success even if device doesn't exist,
        # or log an error. Here we raise 404.
        raise HTTPException(status_code=404, detail=f"Device {request.device_id} not found")
    
    # We only accept telemetry from deployed devices
    if device.status != DeviceStatus.DEPLOYED:
        raise HTTPException(status_code=400, detail="Device must be 'deployed' to send telemetry")

    # Publish to RabbitMQ
    # Convert Pydantic model to dict, excluding base fields for the 'data' part
    telemetry_dict = request.model_dump(exclude={"device_id", "device_name", "timestamp", "location"})
    
    success = rabbitmq_publisher.publish_telemetry(
        device_id=device.id,
        device_name=device.name,
        data=telemetry_dict
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish telemetry")
        
    return ActionResponse(success=True, message="Telemetry received and published")


@router.post("/events", response_model=ActionResponse)
def receive_event(
    request: DeviceEventRequest,
):
    """
    Receive events/alerts from a device and publish to RabbitMQ.
    """
    success = rabbitmq_publisher.publish_device_event(
        device_id=request.device_id,
        device_name=f"Device-{request.device_id}",
        event_type=request.event_type,
        details=request.details or {}
    )
    
    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish event")
    
    return ActionResponse(success=True, message="Event received and published")


@router.get("/{device_id}/type")
def get_device_type(
    device_id: int,
    session: Session = Depends(get_db_session)
):
    """
    Get device type by ID (public endpoint for internal service communication).
    Returns only the device type without sensitive information.
    """
    device = DeviceDAO.get_device_by_id(session, device_id)
    if not device:
        raise HTTPException(status_code=404, detail=f"Device {device_id} not found")
    
    return {"device_id": device_id, "type": device.type.value if device.type else "other"}
