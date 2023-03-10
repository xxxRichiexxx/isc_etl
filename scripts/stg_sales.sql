DECLARE @a date;
set @a = '{0}'
DECLARE @b date;
set @b = '{0}'

EXECUTE [dbo].[хп_ВыгрузкаДляСайта_ПродажиДилеров] 
@НачалоПериода = @a,
@ОкончаниеПериода = @b
