"""
Data Access Layer (DAL) for Device operations.
Handles all database operations for devices.
"""

from typing import List, Optional
from sqlalchemy.orm import Session
from entities.device import Device, DeviceLog, DeviceStatus, DeviceType
from dto.device_dto import DeviceStatusDto, DeviceTypeDto


class DeviceDAO:
    """Data Access Object for Device CRUD operations."""
    
    @staticmethod
    def create_device(session: Session, device: Device) -> bool:
        """
        Create a new device in the database.
        
        Args:
            session: Database session
            device: Device entity to create
            
        Returns:
            True if created successfully, False if serial number already exists
        """
        # Check if serial number already exists
        existing = session.query(Device).filter(
            Device.serial_number == device.serial_number
        ).one_or_none()
        
        if existing:
            return False
        
        session.add(device)
        try:
            session.commit()
            session.refresh(device)
            return True
        except Exception as e:
            print(f"Device creation error: {e}")
            session.rollback()
            return False
    
    @staticmethod
    def get_all_devices(
        session: Session, 
        status: Optional[DeviceStatusDto] = None,
        device_type: Optional[DeviceTypeDto] = None,
        page: int = 1, 
        page_size: int = 20
    ) -> tuple[List[Device], int]:
        """
        Get all devices with optional filtering and pagination.
        
        Args:
            session: Database session
            status: Optional status filter
            device_type: Optional type filter
            page: Page number (1-based)
            page_size: Number of items per page
            
        Returns:
            Tuple of (list of devices, total count)
        """
        query = session.query(Device)
        
        # Apply filters
        if status:
            query = query.filter(Device.status == DeviceStatus(status.value))
        if device_type:
            query = query.filter(Device.type == DeviceType(device_type.value))
        
        # Get total count
        total = query.count()
        
        # Apply pagination
        offset = (page - 1) * page_size
        devices = query.offset(offset).limit(page_size).all()
        
        return devices, total
    
    @staticmethod
    def get_device_by_id(session: Session, device_id: int) -> Optional[Device]:
        """
        Get a device by its ID.
        
        Args:
            session: Database session
            device_id: Device ID to look up
            
        Returns:
            Device if found, None otherwise
        """
        return session.query(Device).filter(Device.id == device_id).one_or_none()
    
    @staticmethod
    def get_device_by_serial(session: Session, serial_number: str) -> Optional[Device]:
        """
        Get a device by its serial number.
        
        Args:
            session: Database session
            serial_number: Device serial number
            
        Returns:
            Device if found, None otherwise
        """
        return session.query(Device).filter(
            Device.serial_number == serial_number
        ).one_or_none()
    
    @staticmethod
    def update_device(session: Session, device_id: int, **kwargs) -> Optional[Device]:
        """
        Update device attributes.
        
        Args:
            session: Database session
            device_id: Device ID to update
            **kwargs: Field names and new values
            
        Returns:
            Updated device if found, None otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return None
        
        # Update allowed fields
        allowed_fields = ['name', 'description', 'location', 'specifications']
        for field, value in kwargs.items():
            if field in allowed_fields and hasattr(device, field):
                setattr(device, field, value)
        
        try:
            session.commit()
            session.refresh(device)
            return device
        except Exception as e:
            session.rollback()
            return None
    
    @staticmethod
    def update_device_status(
        session: Session, 
        device_id: int, 
        new_status: DeviceStatusDto,
        location: Optional[str] = None
    ) -> Optional[Device]:
        """
        Update device status with location change if provided.
        
        Args:
            session: Database session
            device_id: Device ID to update
            new_status: New status value
            location: Optional new location
            
        Returns:
            Updated device if found, None otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return None
        
        old_status = device.status.value if device.status else None
        device.status = DeviceStatus(new_status.value)
        
        if location:
            device.location = location
        
        try:
            session.commit()
            session.refresh(device)
            return device
        except Exception as e:
            session.rollback()
            return None
    
    @staticmethod
    def delete_device(session: Session, device_id: int) -> bool:
        """
        Delete a device (soft delete by setting status to retired).
        
        Args:
            session: Database session
            device_id: Device ID to delete
            
        Returns:
            True if deleted successfully, False otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return False
        
        device.status = DeviceStatus.RETIRED
        try:
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
    
    @staticmethod
    def get_devices_by_status(session: Session, status: DeviceStatusDto) -> List[Device]:
        """
        Get all devices with a specific status.
        
        Args:
            session: Database session
            status: Status to filter by
            
        Returns:
            List of devices with the specified status
        """
        return session.query(Device).filter(
            Device.status == DeviceStatus(status.value)
        ).all()
    
    @staticmethod
    def deploy_device(
        session: Session, 
        device_id: int, 
        deployment_location: str
    ) -> Optional[Device]:
        """
        Deploy a device to the field.
        
        Args:
            session: Database session
            device_id: Device ID to deploy
            deployment_location: Deployment location
            
        Returns:
            Updated device if successful, None otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return None
        
        # Allow deployment from in_stock, reserved, or maintenance status
        allowed_statuses = [DeviceStatus.IN_STOCK, DeviceStatus.RESERVED, DeviceStatus.MAINTENANCE]
        if device.status not in allowed_statuses:
            return None
        
        from datetime import datetime
        device.status = DeviceStatus.DEPLOYED
        device.location = deployment_location
        device.deploy_date = datetime.utcnow()
        
        try:
            session.commit()
            session.refresh(device)
            return device
        except Exception as e:
            session.rollback()
            return None
    
    @staticmethod
    def recall_device(
        session: Session, 
        device_id: int, 
        warehouse_location: Optional[str] = None
    ) -> Optional[Device]:
        """
        Recall a device from the field to stock.
        
        Args:
            session: Database session
            device_id: Device ID to recall
            warehouse_location: Optional new warehouse location
            
        Returns:
            Updated device if successful, None otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return None
        
        if device.status != DeviceStatus.DEPLOYED:
            return None
        
        device.status = DeviceStatus.IN_STOCK
        if warehouse_location:
            device.location = warehouse_location
        device.deploy_date = None
        
        try:
            session.commit()
            session.refresh(device)
            return device
        except Exception as e:
            session.rollback()
            return None
    
    @staticmethod
    def send_to_maintenance(session: Session, device_id: int) -> Optional[Device]:
        """
        Send a device to maintenance.
        
        Args:
            session: Database session
            device_id: Device ID to send to maintenance
            
        Returns:
            Updated device if successful, None otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return None
        
        from datetime import datetime
        device.status = DeviceStatus.MAINTENANCE
        device.last_maintenance_date = datetime.utcnow()
        
        try:
            session.commit()
            session.refresh(device)
            return device
        except Exception as e:
            session.rollback()
            return None

    @staticmethod
    def reserve_device(session: Session, device_id: int) -> Optional[Device]:
        """
        Reserve a device for an order.
        
        Args:
            session: Database session
            device_id: Device ID to reserve
            
        Returns:
            Updated device if successful, None otherwise
        """
        device = session.query(Device).filter(Device.id == device_id).one_or_none()
        if not device:
            return None
        
        if device.status != DeviceStatus.IN_STOCK:
            return None
        
        device.status = DeviceStatus.RESERVED
        
        try:
            session.commit()
            session.refresh(device)
            return device
        except Exception as e:
            session.rollback()
            return None
    
    @staticmethod
    def log_action(
        session: Session,
        device_id: int,
        action: str,
        old_status: Optional[str] = None,
        new_status: Optional[str] = None,
        performed_by: Optional[int] = None,
        notes: Optional[str] = None
    ) -> bool:
        """
        Log a device lifecycle action.
        
        Args:
            session: Database session
            device_id: Device ID
            action: Action performed
            old_status: Previous status
            new_status: New status
            performed_by: User ID who performed the action
            notes: Optional notes
            
        Returns:
            True if logged successfully
        """
        log = DeviceLog(
            device_id=device_id,
            action=action,
            old_status=old_status,
            new_status=new_status,
            performed_by=performed_by,
            notes=notes
        )
        session.add(log)
        try:
            session.commit()
            return True
        except Exception as e:
            session.rollback()
            return False
