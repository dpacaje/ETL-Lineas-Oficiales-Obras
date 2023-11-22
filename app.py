import os
import MySQLdb as mysql
from urllib import request
from dotenv import load_dotenv

load_dotenv()

conexion = mysql.connect(host=os.getenv('DB_HOST'), user=os.getenv('DB_USER'), passwd=os.getenv('DB_PASSWORD'), db=os.getenv('DB_NAME'))
cur = conexion.cursor()

query = """SELECT 
solicitud_id,
certificado_id,
por_calle1,
lf_xcalles1,
tipo_via1,
lf_xvia1,
check_sitio_eriazo1,
check_borde_distancia_1,
check_calzada_1,
distancia_lo1,
lf_borde_lo1,
distancia_lo1_2,
lf_borde_lo1_2,
eje_calzada1,
lf_distancia_eje1,
eje_calzada1_2,
lf_distancia_eje1_2,
antejardin1,
lf_jardin1,
calzada1,
lf_calzada1,
calzada1_2,
lf_calzada1_2,
por_calle2,
lf_xcalles2,
tipo_via2,
lf_xvia2,
check_sitio_eriazo2,
check_borde_distancia_2,
check_calzada_2,
distancia_lo2,
lf_borde_lo2,
distancia_lo2_2,
lf_borde_lo2_2,
eje_calzada2,
lf_distancia_eje2,
eje_calzada2_2,
lf_distancia_eje2_2,
antejardin2,
lf_jardin2,
calzada2,
lf_calzada2,
calzada2_2,
lf_calzada2_2,
por_calle3,
lf_xcalles3,
tipo_via3,
lf_xvia3,
check_sitio_eriazo3,
check_borde_distancia_3,
check_calzada_3,
distancia_lo3,
lf_borde_lo3,
distancia_lo3_2,
lf_borde_lo3_2,
eje_calzada3,
lf_distancia_eje3,
eje_calzada3_2,
lf_distancia_eje3_2,
antejardin3,
lf_jardin3,
calzada3,
lf_calzada3,
calzada3_2,
lf_calzada3_2,
por_calle4,
lf_xcalles4,
tipo_via4,
lf_xvia4,
check_sitio_eriazo4,
check_borde_distancia_4,
check_calzada_4,
distancia_lo4,
lf_borde_lo4,
distancia_lo4_2,
lf_borde_lo4_2,
eje_calzada4,
lf_distancia_eje4,
eje_calzada4_2,
lf_distancia_eje4_2,
antejardin4,
lf_jardin4,
calzada4,
lf_calzada4,
calzada4_2,
lf_calzada4_2
FROM certificado WHERE tipo = 4 AND por_calle1 IS NOT NULL""";

cur.execute(query)
certificados = cur.fetchall()

if len(certificados) < 1 :
    print('No hay Certificados')
else :
    print('Hay ', len(certificados), 'certificados.')
    for fila in certificados:
        print(fila[0])
        subquery = "INSERT INTO lineas_oficiales_etl (solicitud_id,certificado_id,correlativo,titulo_calle,descripcion_calle,titulo_tipo_via,id_tipo_via,check_sitio_eriazo,check_distancias,check_calzada,titulo_distancia_lo_1,descripcion_distancia_lo_1,titulo_distancia_lo_2,descripcion_distancia_lo_2,titulo_distancia_eje_1,descripcion_distancia_eje_1,titulo_distancia_eje_2,descripcion_distancia_eje_2,titulo_antejardin,descripcion_antejardin,titulo_calzada_1,descripcion_calzada_1,titulo_calzada_2,descripcion_calzada_2,fecha_creacion) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)";

        datos_insertar_1 = (fila[0], fila[1], 1, fila[2], fila[3], 'Tipo de Vía', fila[5], fila[6], fila[7], fila[8], fila[9], fila[10], fila[11], fila[12], fila[13], fila[14], fila[15], fila[16], fila[17], fila[18], fila[19], fila[20], fila[21], fila[22], 20231121)
        datos_insertar_2 = (fila[0], fila[1], 2, fila[23], fila[24], 'Tipo de Vía', fila[26], fila[27], fila[28], fila[29], fila[30], fila[31], fila[32], fila[33], fila[34], fila[35], fila[36], fila[37], fila[38], fila[39], fila[40], fila[41], fila[42], fila[43], 20231121)
        datos_insertar_3 = (fila[0], fila[1], 3, fila[44], fila[45], 'Tipo de Vía', fila[47], fila[48], fila[49], fila[50], fila[51], fila[52], fila[53], fila[54], fila[55], fila[56], fila[57], fila[58], fila[59], fila[60], fila[61], fila[62], fila[63], fila[64], 20231121)
        datos_insertar_4 = (fila[0], fila[1], 4, fila[65], fila[66], 'Tipo de Vía', fila[68], fila[69], fila[70], fila[71], fila[72], fila[73], fila[74], fila[75], fila[76], fila[77], fila[78], fila[79], fila[80], fila[81], fila[82], fila[83], fila[84], fila[67], 20231121)

        cur.execute(subquery, datos_insertar_1)
        conexion.commit()

        cur.execute(subquery, datos_insertar_2)
        conexion.commit()

        cur.execute(subquery, datos_insertar_3)
        conexion.commit()

        cur.execute(subquery, datos_insertar_4)
        conexion.commit()

print('Fin')
conexion.close()
