def scrap_fuerte(Sopa_produc):
    """ Extrae datos de productos frescos desde un objeto BeautifulSoup. 

    Args: sopa_productos (BeautifulSoup): Objeto BeautifulSoup que contiene la información de los productos. 
    
    Returns: lista_productos (list): 
                                    Lista de listas, donde cada sublista contiene datos de un producto. 
    La función extrae el nombre del producto, el precio, el descuento, y el precio unitario de cada producto. """
    
    lista_productos = []
    for e in Sopa_produc:
        elemento = []
        
        nombre_producto = e.find('h3').get_text()
        elemento.append(nombre_producto)
        
        precio = e.findAll('span', class_='price')[1].get_text()
        elemento.append(precio)
        
        votos = e.findAll('span', class_='votes hide-for-small')[0].get_text()
        elemento.append(votos)
        
        descripcion = e.find('meta', {'itemprop': 'description'}).get('content')
        elemento.append(descripcion)
        
        rating_meta = e.find('meta', {'itemprop': 'ratingValue'})
        valor_rating = rating_meta.get('content') if rating_meta else None
        elemento.append(valor_rating)
        
        lista_productos.append(elemento)
    
    return lista_productos


def productos_frescos(sopa_productos):
    """
    Extrae datos de productos frescos desde un objeto BeautifulSoup.

    Args:
        sopa_productos (BeautifulSoup): Objeto BeautifulSoup que contiene la información de los productos.

    Returns:
        lista_productos (list): Lista de listas, donde cada sublista contiene datos de un producto.
    
    La función extrae el nombre del producto, el precio, el descuento, y el precio unitario de cada producto.
    """
    lista_productos = []
    for e in sopa_productos:
        elemento = []
        
        # Extraer el nombre del producto
        nombre_producto = e.find('h2').get_text()
        elemento.append(nombre_producto)
        
        # Extraer el precio del producto
        precio = e.findAll('span', class_='value')[0].get_text()
        elemento.append(precio)
        
        # Extraer el descuento del producto
        descuento = e.findAll('span', class_='marker')[0].get_text()
        elemento.append(descuento)
        
        # Extraer el precio unitario del producto
        precio_unitario = e.findAll('div', class_='unit-price-row price')[0].get_text()
        elemento.append(precio_unitario)
        
        lista_productos.append(elemento)
    
    return lista_productos


