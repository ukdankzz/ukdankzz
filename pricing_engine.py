import json
import math
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import requests
from database import DatabaseManager

logger = logging.getLogger(__name__)

class PricingEngine:
    """
    Centralized pricing engine that handles all discount logic consistently.
    
    Discount precedence order:
    1. Base price
    2. Bundle discounts (buy X get Y)
    3. Item-level/collection promotions  
    4. Coupon codes
    5. Flash sales (time-limited)
    
    Non-stackable by default unless explicitly stackable.
    """
    
    def __init__(self, db: DatabaseManager):
        self.db = db
        
    def calculate_cart_pricing(self, cart_items: List[Dict], user_id: int, 
                              coupon_code: str = None, timestamp: datetime = None) -> Dict:
        """
        Calculate final pricing for entire cart with all applicable discounts.
        
        Args:
            cart_items: List of cart items with name, size, price, quantity
            user_id: User ID for personalized discounts
            coupon_code: Optional coupon code to apply
            timestamp: Timestamp for flash sales (defaults to now)
            
        Returns:
            Dict with pricing breakdown and totals
        """
        if not timestamp:
            timestamp = datetime.now()
            
        try:
            # Initialize pricing breakdown
            pricing = {
                'items': [],
                'subtotal': 0.0,
                'bundle_discount': 0.0,
                'promotion_discount': 0.0,
                'coupon_discount': 0.0,
                'flash_discount': 0.0,
                'total_discount': 0.0,
                'final_total_gbp': 0.0,
                'final_total_ltc': 0.0,
                'ltc_rate': 0.0,
                'discounts_applied': [],
                'coupon_valid': False,
                'coupon_message': ""
            }
            
            # Get active promotions
            promotions = self.db.get_active_promotions()
            
            # Step 1: Calculate base prices and bundle discounts
            processed_items = self._apply_bundle_discounts(cart_items, promotions)
            
            # Step 1.5: Apply edibles mix-and-match deal (any 3 for £40)
            processed_items = self._apply_edibles_mix_match(processed_items)
            
            # Step 2: Apply item-level and collection promotions
            processed_items = self._apply_item_promotions(processed_items, promotions)
            
            # Step 3: Apply flash sales
            processed_items = self._apply_flash_sales(processed_items, promotions, timestamp)
            
            # Calculate subtotal and discounts
            for item in processed_items:
                pricing['items'].append(item)
                pricing['subtotal'] += item['original_price'] * item['quantity']
                pricing['bundle_discount'] += item.get('bundle_discount', 0.0)
                pricing['promotion_discount'] += item.get('promotion_discount', 0.0)
                pricing['flash_discount'] += item.get('flash_discount', 0.0)
            
            # Calculate current total before coupon
            current_total = pricing['subtotal'] - pricing['bundle_discount'] - pricing['promotion_discount'] - pricing['flash_discount']
            
            # Step 4: Apply coupon if provided
            if coupon_code:
                coupon_result = self._apply_coupon(coupon_code, user_id, current_total)
                pricing['coupon_discount'] = coupon_result['discount']
                pricing['coupon_valid'] = coupon_result['valid']
                pricing['coupon_message'] = coupon_result['message']
                if coupon_result['valid']:
                    pricing['discounts_applied'].append(f"Coupon: {coupon_code}")
            
            # Calculate final totals
            pricing['total_discount'] = (pricing['bundle_discount'] + 
                                       pricing['promotion_discount'] + 
                                       pricing['coupon_discount'] + 
                                       pricing['flash_discount'])
            
            pricing['final_total_gbp'] = max(0, pricing['subtotal'] - pricing['total_discount'])
            
            # Convert to LTC
            ltc_conversion = self._convert_to_ltc(pricing['final_total_gbp'])
            pricing['final_total_ltc'] = ltc_conversion['amount']
            pricing['ltc_rate'] = ltc_conversion['rate']
            
            logger.info(f"✅ Pricing calculated for user {user_id}: £{pricing['final_total_gbp']:.2f}")
            return pricing
            
        except Exception as e:
            logger.error(f"❌ Error calculating cart pricing: {e}")
            # Return safe fallback
            return self._fallback_pricing(cart_items)
    
    def _apply_bundle_discounts(self, cart_items: List[Dict], promotions: List[Dict]) -> List[Dict]:
        """Apply bundle discounts (buy X get Y free)"""
        processed_items = []
        
        for item in cart_items:
            processed_item = {
                'name': item['name'],
                'size': item['size'],
                'quantity': 1,  # Each cart item is a single unit
                'original_price': float(item['price']),
                'final_price': float(item['price']),
                'bundle_discount': 0.0,
                'promotion_discount': 0.0,
                'flash_discount': 0.0,
                'discounts_applied': []
            }
            
            # Check for bundle promotions
            for promo in promotions:
                if promo['type'] == 'bundle' and self._item_matches_promotion(item, promo):
                    buy_qty = promo['buy_qty']
                    get_qty = promo['get_qty']
                    item_qty = processed_item['quantity']
                    
                    if item_qty >= buy_qty:
                        # Calculate how many free items they get
                        free_sets = item_qty // buy_qty
                        free_items = min(free_sets * get_qty, item_qty)
                        bundle_discount = free_items * processed_item['original_price']
                        
                        processed_item['bundle_discount'] = bundle_discount
                        processed_item['final_price'] = max(0, processed_item['original_price'] - (bundle_discount / item_qty))
                        processed_item['discounts_applied'].append(f"Bundle: {promo['name']}")
                        break
            
            processed_items.append(processed_item)
        
        return processed_items
    
    def _apply_edibles_mix_match(self, items: List[Dict]) -> List[Dict]:
        """Apply special edibles mix-and-match deal: any 3 for £40"""
        # Find all Relax 500MG edibles in cart
        edible_indices = []
        for i, item in enumerate(items):
            if "Relax 500MG" in item['name']:
                edible_indices.append(i)
        
        # Apply discount in groups of 3
        edibles_processed = 0
        for i in range(0, len(edible_indices), 3):
            # Get up to 3 edibles for this group
            group = edible_indices[i:i+3]
            
            if len(group) >= 3:  # Only apply if we have 3 or more
                # Calculate total price for these 3 edibles (should be 3 x £15 = £45)
                total_normal_price = sum(items[idx]['final_price'] for idx in group)
                target_price = 40.0  # £40 for any 3
                
                # Calculate discount per item to reach £40 total
                discount_per_item = (total_normal_price - target_price) / 3
                
                # Apply discount to each item in the group
                for idx in group:
                    items[idx]['promotion_discount'] += discount_per_item
                    items[idx]['final_price'] = max(0, items[idx]['final_price'] - discount_per_item)
                    items[idx]['discounts_applied'].append("Mix & Match: Any 3 Edibles £40")
                
                edibles_processed += 3
        
        return items
    
    def _apply_item_promotions(self, items: List[Dict], promotions: List[Dict]) -> List[Dict]:
        """Apply item-level and collection promotions"""
        for item in items:
            for promo in promotions:
                if promo['type'] in ['item', 'collection'] and self._item_matches_promotion(item, promo):
                    if promo['percent_off'] > 0:
                        discount = (item['final_price'] * item['quantity']) * (promo['percent_off'] / 100)
                    elif promo['amount_off'] > 0:
                        discount = min(promo['amount_off'], item['final_price'] * item['quantity'])
                    else:
                        continue
                    
                    item['promotion_discount'] += discount
                    item['final_price'] = max(0, item['final_price'] - (discount / item['quantity']))
                    item['discounts_applied'].append(f"Promo: {promo['name']}")
                    
                    # Only apply first matching promotion unless stackable
                    if not promo.get('stackable', False):
                        break
        
        return items
    
    def _apply_flash_sales(self, items: List[Dict], promotions: List[Dict], timestamp: datetime) -> List[Dict]:
        """Apply flash sale discounts"""
        for item in items:
            for promo in promotions:
                if (promo['type'] == 'flash' and 
                    self._is_flash_sale_active(promo, timestamp) and 
                    self._item_matches_promotion(item, promo)):
                    
                    if promo['percent_off'] > 0:
                        discount = (item['final_price'] * item['quantity']) * (promo['percent_off'] / 100)
                    elif promo['amount_off'] > 0:
                        discount = min(promo['amount_off'], item['final_price'] * item['quantity'])
                    else:
                        continue
                    
                    item['flash_discount'] += discount
                    item['final_price'] = max(0, item['final_price'] - (discount / item['quantity']))
                    item['discounts_applied'].append(f"Flash Sale: {promo['name']}")
                    
                    # Only apply first flash sale
                    break
        
        return items
    
    def _apply_coupon(self, coupon_code: str, user_id: int, cart_total: float) -> Dict:
        """Apply coupon discount"""
        try:
            validation = self.db.validate_coupon(coupon_code, user_id, cart_total)
            
            if not validation['valid']:
                return {
                    'valid': False,
                    'discount': 0.0,
                    'message': validation['message']
                }
            
            coupon = validation['coupon']
            
            if coupon['type'] == 'percent':
                discount = cart_total * (coupon['value'] / 100)
            else:  # fixed amount
                discount = min(coupon['value'], cart_total)
            
            return {
                'valid': True,
                'discount': discount,
                'message': f"Coupon {coupon_code} applied: £{discount:.2f} off"
            }
            
        except Exception as e:
            logger.error(f"❌ Error applying coupon: {e}")
            return {
                'valid': False,
                'discount': 0.0,
                'message': "Error applying coupon"
            }
    
    def _item_matches_promotion(self, item: Dict, promo: Dict) -> bool:
        """Check if an item matches a promotion's target criteria"""
        target_products = promo.get('target_product_ids', [])
        
        # If no target products specified, applies to all
        if not target_products:
            return True
            
        # Check if item name matches any target product
        item_name = item['name'].lower()
        for target in target_products:
            if target.lower() in item_name or item_name in target.lower():
                return True
        
        return False
    
    def _is_flash_sale_active(self, promo: Dict, timestamp: datetime) -> bool:
        """Check if flash sale is currently active"""
        start_time = promo.get('start_at')
        end_time = promo.get('end_at')
        
        if not end_time:
            return True
            
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        return timestamp <= end_time
    
    def _convert_to_ltc(self, gbp_amount: float) -> Dict:
        """Convert GBP to LTC using current exchange rate"""
        try:
            # Get current LTC price from external API
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price?ids=litecoin&vs_currencies=gbp',
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                ltc_price_gbp = data['litecoin']['gbp']
                ltc_amount = gbp_amount / ltc_price_gbp
                
                # Round up to 8 decimal places (standard crypto precision)
                ltc_amount = math.ceil(ltc_amount * 100000000) / 100000000
                
                return {
                    'amount': ltc_amount,
                    'rate': ltc_price_gbp
                }
            else:
                raise Exception(f"API error: {response.status_code}")
                
        except Exception as e:
            logger.warning(f"⚠️ Error getting LTC rate, using fallback: {e}")
            # Fallback rate
            fallback_rate = 50.0  # Conservative estimate
            ltc_amount = math.ceil((gbp_amount / fallback_rate) * 100000000) / 100000000
            
            return {
                'amount': ltc_amount,
                'rate': fallback_rate
            }
    
    def _fallback_pricing(self, cart_items: List[Dict]) -> Dict:
        """Safe fallback pricing if main calculation fails"""
        subtotal = sum(float(item['price']) * item.get('quantity', 1) for item in cart_items)
        
        return {
            'items': cart_items,
            'subtotal': subtotal,
            'bundle_discount': 0.0,
            'promotion_discount': 0.0,
            'coupon_discount': 0.0,
            'flash_discount': 0.0,
            'total_discount': 0.0,
            'final_total_gbp': subtotal,
            'final_total_ltc': subtotal / 50.0,  # Conservative fallback
            'ltc_rate': 50.0,
            'discounts_applied': [],
            'coupon_valid': False,
            'coupon_message': ""
        }

    def calculate_coupon_discount(self, coupon: Dict, cart_total: float) -> float:
        """Calculate discount amount for a coupon"""
        try:
            if coupon['type'] == 'percent':
                discount = cart_total * (float(coupon['value']) / 100)
            else:  # fixed amount
                discount = min(float(coupon['value']), cart_total)
            
            return discount
            
        except Exception as e:
            logger.error(f"❌ Error calculating coupon discount: {e}")
            return 0.0

    def preview_promotion_impact(self, promotion_data: Dict, sample_cart: List[Dict] = None) -> Dict:
        """Preview how a promotion would affect pricing for admin review"""
        if not sample_cart:
            # Use a default sample cart for testing
            sample_cart = [
                {'name': 'Sample Product', 'size': '3.5g', 'price': 25.0, 'quantity': 2}
            ]
        
        # Calculate pricing without promotion
        original_pricing = self.calculate_cart_pricing(sample_cart, 999999)  # Test user
        
        # Temporarily create promotion to test impact
        test_promotions = [promotion_data]
        processed_items = self._apply_bundle_discounts(sample_cart, test_promotions)
        processed_items = self._apply_item_promotions(processed_items, test_promotions)
        
        discount_amount = sum(item.get('bundle_discount', 0) + item.get('promotion_discount', 0) 
                            for item in processed_items)
        
        return {
            'original_total': original_pricing['final_total_gbp'],
            'discount_amount': discount_amount,
            'final_total': original_pricing['final_total_gbp'] - discount_amount,
            'discount_percentage': (discount_amount / original_pricing['subtotal']) * 100 if original_pricing['subtotal'] > 0 else 0,
            'affected_items': len([item for item in processed_items if item.get('bundle_discount', 0) + item.get('promotion_discount', 0) > 0])
        }